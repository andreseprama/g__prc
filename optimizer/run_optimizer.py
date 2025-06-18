from __future__ import annotations
import logging
from datetime import date
from typing import Optional, List, Tuple, Union, Set
import faulthandler
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from ortools.constraint_solver import pywrapcp
from itertools import zip_longest, chain

from .prepare_input import prepare_input_dataframe
from .subset_selection import selecionar_servicos_e_trailers_compativeis
from .setup_model import setup_routing_model
from .constraints import apply_all_constraints
from .persist_results import persist_routes
from backend.solver.geocode import fetch_and_store_city
from backend.solver.utils import norm
from backend.solver.optimizer.city_mapping import get_unique_cities
from backend.solver.optimizer.solve_model import solve_with_params
from backend.solver.distance import _norm, get_coords, register_coords
from backend.solver.optimizer.cluster import agrupar_por_cluster_geografico
import gc
import time

faulthandler.enable()
logger = logging.getLogger(__name__)

def _get_ceu_capacities(trailers: List[dict]) -> List[int]:
    capacities = []
    for t in trailers:
        if t.get("ceu_max") is not None:
            capacities.append(int(float(t["ceu_max"]) * 10))
        elif t.get("cat") and t["cat"].get("ceu_max") is not None:
            capacities.append(int(float(t["cat"]["ceu_max"]) * 10))
        else:
            capacities.append(0)
    return capacities


async def geocode_all_unique_cities(sess, df: pd.DataFrame):
    cidades = set()

    # scheduled_base sempre entra
    cidades.update(_norm(c) for c in df["scheduled_base"].dropna().unique())

    # load/unload: normalizar todas, independentemente de *_is_base
    cidades.update(_norm(c) for c in df["load_city"].dropna().unique())
    cidades.update(_norm(c) for c in df["unload_city"].dropna().unique())

    for cidade in cidades:
        try:
            await fetch_and_store_city(sess, cidade)
        except Exception as e:
            logger.warning(f"⚠️ Falha ao geocodificar {cidade}: {e}")


async def optimize(
    sess: AsyncSession,
    dia: date,
    registry_trailer: Optional[str] = None,
    categoria_filtrada: Optional[List[str]] = None,
    debug: bool = False,
    safe: bool = False,
    max_voltas: int = 10
) -> Union[List[int], Tuple[List[int], pd.DataFrame]]:
    df, trailers, base_map = await prepare_input_dataframe(sess, dia, registry_trailer)

    if "service_reg" not in df.columns:
        raise ValueError("❌ Faltando coluna obrigatória: service_reg")
    if df.empty or not trailers:
        logger.warning("⚠️ Sem dados elegíveis ou trailers disponíveis.")
        return []

    await geocode_all_unique_cities(sess, df)

    bases = df["scheduled_base"].dropna().unique()
    coords_map: dict[str, Tuple[float, float]] = {}
    for base in bases:
        coords = get_coords(_norm(base))
        if coords is not None:
            coords_map[base] = coords
    register_coords(coords_map)

    rota_ids_total = []
    trailers_restantes = trailers
    services_alocados: Set[str] = set()

    if "rota_id" in df.columns:
        duplicates = df.groupby("service_reg")["rota_id"].nunique()
        invalid_services = duplicates[duplicates > 1]
        if not invalid_services.empty:
            raise ValueError(f"❌ Serviços em mais de uma rota: {invalid_services}")

    clusters_load = agrupar_por_cluster_geografico(df, tipo="load", n_clusters=4)
    clusters_unload = agrupar_por_cluster_geografico(df, tipo="unload", n_clusters=2)

    clusters = list(chain.from_iterable(zip_longest(clusters_load, clusters_unload)))
    clusters = [c for c in clusters if c is not None]

    for rodada, cluster_df in enumerate(clusters, start=1):
        df_restante = cluster_df[~cluster_df["service_reg"].isin(services_alocados)].reset_index(drop=True)
        logger.info(f"📦 Cluster {rodada}: {len(df_restante)} serviços restantes para alocar")

        if df_restante.empty or not trailers_restantes:
            continue

        df_usado, df_restante, trailers_usados, _ = selecionar_servicos_e_trailers_compativeis(df_restante, trailers_restantes)
        if df_usado.empty or not trailers_usados:
            logger.info(f"⛔ Sem trailers compatíveis para rodada {rodada} ({len(df_restante)} serviços)")
            continue

        # ✅ Validação das colunas obrigatórias antes de setup_routing_model
        required_cols = ["id", "service_reg", "ceu_int", "load_city", "unload_city", "scheduled_base"]
        missing_cols = [col for col in required_cols if col not in df_usado.columns]

        if missing_cols:
            logger.error(f"❌ df_usado está faltando colunas obrigatórias: {missing_cols}")
            continue  # Pula essa rodada            
            
        # ✅ Validação de colunas obrigatórias nos trailers
        required_trailer_keys = ["id", "base_city", "ceu_max"]
        for t in trailers_usados:
            missing = [k for k in required_trailer_keys if k not in t]
            if missing:
                logger.error(f"❌ Trailer {t.get('id', '??')} com chaves faltando: {missing}")
                continue  # ou `raise ValueError(...)` se for ambiente de staging

        logger.info("📦 Serviços a transportar nesta rodada:")
        for _, row in df_usado.iterrows():
            logger.info(
                f"🧾 ID={row.get('id')}, REG={row.get('service_reg')}, MAT={row.get('registry')}, "
                f"BASE={row.get('scheduled_base')}, CIDADE={row.get('load_city')} → {row.get('unload_city')}, "
                f"CEU={row.get('ceu_int')}"
            )

        logger.debug(f"🔍 df_usado.shape: {df_usado.shape}")
        logger.debug(f"🔍 Columns: {list(df_usado.columns)}")
        if not df_usado.empty:
            logger.debug(f"🔍 Primeira linha: {df_usado.iloc[0].to_dict()}")

        try:
            routing, manager, starts, dist_matrix, df_idx_map = setup_routing_model(df_usado, trailers_usados, debug=debug)
        except Exception as e:
            logger.error(f"❌ Erro ao preparar modelo de rota: {e}")
            continue

        apply_all_constraints(
            routing,
            manager,
            df_usado,
            trailers_usados,
            n_services=len(df_usado),
            depot_indices=starts,
            distance_matrix=dist_matrix,
            constraint_weights={"ceu": 1.0},
            enable_pickup_pairs=True,
        )

        strategy = "tabu" if len(trailers_usados) > 3 else "guided"
        solution = solve_with_params(
            routing,
            manager,
            time_limit_sec=120,
            log_search=True,
            first_solution_strategy="cheapest",
            local_search_metaheuristic=strategy,
        )

        if solution is None:
            logger.warning(f"❌ Nenhuma solução encontrada na rodada {rodada} com 'cheapest'. Tentando fallback com 'savings'.")
            solution = solve_with_params(
                routing,
                manager,
                time_limit_sec=60,
                log_search=True,
                first_solution_strategy="savings",
                local_search_metaheuristic=strategy,
            )

        if solution is None:
            logger.warning(f"❌ Nenhuma solução encontrada na rodada {rodada} após fallback.")
            continue

        unique_cities = get_unique_cities(df_usado, trailers_usados)
        routes: List[Tuple[int, List[int]]] = []
        for v in range(len(trailers_usados)):
            idx = routing.Start(v)
            path = []
            total_km = 0
            while not routing.IsEnd(idx):
                node = manager.IndexToNode(idx)
                path.append(node)
                logger.debug(f"🚏 Veículo {v} → nó {node} = {unique_cities[node]}")
                next_idx = solution.Value(routing.NextVar(idx))
                if not routing.IsEnd(next_idx):
                    from_node = manager.IndexToNode(idx)
                    to_node = manager.IndexToNode(next_idx)
                    total_km += dist_matrix[from_node][to_node]
                idx = next_idx
            if path:
                logger.info(f"🛳️ Veículo {v} → rota = {path} → Total km: {total_km:.2f}")
                if debug:
                    agrupamento = {}
                    for node in path:
                        cidade = unique_cities[node]
                        agrupamento[cidade] = agrupamento.get(cidade, 0) + 1
                    agrupado_str = ", ".join(f"{c}: {n}" for c, n in agrupamento.items())
                    logger.debug(f"🩹 Veículo {v} → agrupamento por cidade: {agrupado_str}")
                routes.append((v, path))

        rota_ids = await persist_routes(sess, dia, df_usado, routes, trailer_starts=starts, trailers=trailers_usados, df_idx_map=df_idx_map)
        rota_ids_total.extend(rota_ids)
        trailers_restantes = [t for t in trailers_restantes if t not in trailers_usados]
        services_alocados.update(df_usado["service_reg"].unique())

        # Cleanup to prevent memory overflow or segfault
        del routing
        del manager
        gc.collect()
        time.sleep(0.2)

    return rota_ids_total if not debug else (rota_ids_total, df)
