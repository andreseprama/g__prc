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
from .subset_selection import selecionar_servicos_e_trailers_compatíveis
from .setup_model import setup_routing_model
from .constraints import apply_all_constraints
from .persist_results import persist_routes
from backend.solver.geocode import fetch_and_store_city
from backend.solver.utils import norm
from backend.solver.optimizer.city_mapping import get_unique_cities
from backend.solver.optimizer.solve_model import solve_with_params
from backend.solver.distance import _norm, get_coords, register_coords
from backend.solver.optimizer.cluster import agrupar_por_cluster_geografico

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


async def geocode_all_unique_cities(sess, df):
    cidades = set()
    for _, row in df.iterrows():
        if row["load_is_base"] and pd.notnull(row["scheduled_base"]):
            cidades.add(_norm(row["scheduled_base"]))
        else:
            cidades.add(_norm(row["load_city"]))
        if row["unload_is_base"] and pd.notnull(row["scheduled_base"]):
            cidades.add(_norm(row["scheduled_base"]))
        else:
            cidades.add(_norm(row["unload_city"]))

    for cidade in cidades:
        try:
            await fetch_and_store_city(sess, cidade)
        except Exception as e:
            logger.warning(f"⚠️ Falha ao geocodificar {cidade}: {e}")


async def optimize(sess: AsyncSession, dia: date, registry_trailer: Optional[str] = None, categoria_filtrada: Optional[List[str]] = None, debug: bool = False, safe: bool = False, max_voltas: int = 10) -> Union[List[int], Tuple[List[int], pd.DataFrame]]:
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

    duplicates = df.groupby("service_reg")["rota_id"].nunique()
    invalid_services = duplicates[duplicates > 1]
    if not invalid_services.empty:
        raise ValueError(f"❌ Serviços em mais de uma rota: {invalid_services}")

    clusters_load = agrupar_por_cluster_geografico(df, tipo="load", n_clusters=4)
    clusters_unload = agrupar_por_cluster_geografico(df, tipo="unload", n_clusters=2)

    # Intercalar os clusters
    clusters = list(chain.from_iterable(zip_longest(clusters_load, clusters_unload)))
    clusters = [c for c in clusters if c is not None]

    for rodada, cluster_df in enumerate(clusters, start=1):
        df_restante = cluster_df[~cluster_df["service_reg"].isin(services_alocados)].reset_index(drop=True)
        logger.info(f"📦 Cluster {rodada}: {len(df_restante)} serviços restantes para alocar")

        if df_restante.empty or not trailers_restantes:
            continue

        df_usado, df_restante, trailers_usados = selecionar_servicos_e_trailers_compatíveis(df_restante, trailers_restantes)
        if df_usado.empty or not trailers_usados:
            continue

        try:
            routing, manager, starts, dist_matrix = setup_routing_model(df_usado, trailers_usados, debug=debug)
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

        solution = solve_with_params(routing, manager, time_limit_sec=120, log_search=True)
        if solution is None:
            logger.warning(f"❌ Nenhuma solução encontrada na rodada {rodada}.")
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

        rota_ids = await persist_routes(sess, dia, df_usado, routes, trailer_starts=starts, trailers=trailers_usados)
        rota_ids_total.extend(rota_ids)
        trailers_restantes = [t for t in trailers_restantes if t not in trailers_usados]
        services_alocados.update(df_usado["service_reg"].unique())

    return rota_ids_total if not debug else (rota_ids_total, df)