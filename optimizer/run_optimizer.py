# backend/solver/optimizer/run_optimizer.py (com geocodificação antecipada e correções finais)

from __future__ import annotations
import logging
from datetime import date
from typing import Optional, List, Tuple, Union
import faulthandler
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from .prepare_input import prepare_input_dataframe
from .subset_selection import selecionar_servicos_e_trailers_compatíveis
from .setup_model import setup_routing_model
from .constraints import apply_all_constraints
from .persist_results import persist_routes
from backend.solver.geocode import fetch_and_store_city
from backend.solver.utils import norm
from backend.solver.optimizer.city_mapping import get_unique_cities

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
    cities = set(norm(c) for c in df["load_city"].tolist() + df["unload_city"].tolist())
    for city in cities:
        try:
            await fetch_and_store_city(sess, city)
        except Exception as e:
            logger.warning(f"⚠️ Falha ao geocodificar {city}: {e}")


async def optimize(sess: AsyncSession, dia: date, registry_trailer: Optional[str] = None, categoria_filtrada: Optional[List[str]] = None, debug: bool = False, safe: bool = False, max_voltas: int = 10) -> Union[List[int], Tuple[List[int], pd.DataFrame]]:
    df, trailers, base_map = await prepare_input_dataframe(sess, dia, registry_trailer)
    if df.empty or not trailers:
        logger.warning("⚠️ Sem dados elegíveis ou trailers disponíveis.")
        return []

    await geocode_all_unique_cities(sess, df)

    rota_ids_total = []
    rodada = 1
    df_restante = df
    trailers_restantes = trailers

    while not df_restante.empty and trailers_restantes and rodada <= max_voltas:
        df_usado, df_restante, trailers_usados = selecionar_servicos_e_trailers_compatíveis(df_restante, trailers_restantes)
        if df_usado.empty or not trailers_usados:
            break

        try:
            routing, manager, starts, dist_matrix = setup_routing_model(df_usado, trailers_usados)
        except Exception as e:
            logger.error(f"❌ Erro ao preparar modelo de rota: {e}")
            break

        apply_all_constraints(
            routing,
            manager,
            df_usado,
            trailers_usados,
            n_services=len(df_usado),
            depot_indices=starts,
            distance_matrix=dist_matrix,
            constraint_weights={"ceu": 1.0},
            enable_pickup_pairs=False,
        )

        search = pywrapcp.DefaultRoutingSearchParameters()
        search.time_limit.seconds = 30
        search.log_search = debug
        search.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.SAVINGS
        search.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH

        solution = routing.SolveWithParameters(search)
        if solution is None:
            logger.warning("❌ Nenhuma solução encontrada na rodada %d.", rodada)
            break

        unique_cities = get_unique_cities(df_usado, trailers_usados)

        routes: List[Tuple[int, List[int]]] = []
        for v in range(len(trailers_usados)):
            idx = routing.Start(v)
            path = []
            distance = 0
            while not routing.IsEnd(idx):
                node = manager.IndexToNode(idx)
                path.append(node)
                logger.debug(f"🚏 Veículo {v} → nó {node} = {unique_cities[node]}")
                next_idx = solution.Value(routing.NextVar(idx))
                distance += routing.GetArcCostForVehicle(idx, next_idx, v)
                idx = next_idx
            if path:
                logger.info(f"🛣️ Veículo {v} → distância total = {distance} km")
                routes.append((v, path))

        rota_ids = await persist_routes(sess, dia, df_usado, routes, trailer_starts=starts, trailers=trailers_usados)
        rota_ids_total.extend(rota_ids)
        trailers_restantes = [t for t in trailers_restantes if t not in trailers_usados]
        rodada += 1

    return rota_ids_total if not debug else (rota_ids_total, df)
