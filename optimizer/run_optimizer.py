# backend/solver/optimizer/run_optimizer.py
from __future__ import annotations

import logging
from datetime import date
from typing import Optional, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from .prepare_input import prepare_input_dataframe
from backend.solver.routing import (
    create_demand_callbacks,
    add_dimensions_and_constraints,
)
from .persist_results import persist_routes

logger = logging.getLogger(__name__)


async def optimize(
    sess: AsyncSession,
    dia: date,
    matricula: Optional[str] = None,
    categoria_filtrada: Optional[List[str]] = None,
) -> List[int]:
    # ───────────────────────────────────────────────────────
    # 1) Carrega serviços + trailers
    # ───────────────────────────────────────────────────────
    df, trailers, base_map = await prepare_input_dataframe(sess, dia, matricula)
    if df.empty:
        logger.warning("⚠️ Nenhum serviço elegível.")
        return []
    if not trailers:
        logger.warning("⚠️ Nenhum trailer activo.")
        return []

    if categoria_filtrada:
        from .trailer_routing import filter_services_by_category

        df = filter_services_by_category(df, categoria_filtrada, base_map)
        if df.empty:
            logger.warning("⚠️ Nenhum serviço após filtro.")
            return []

    # 2 nós por serviço: 1 depósito fictício + pickups + deliveries
    n_srv = len(df)
    n_veh = len(trailers)
    depot = 0

    # 1 nó de depósito + n_srv pickups + n_srv deliveries
    n_nodes = 1 + 2 * n_srv
    starts = [depot] * n_veh
    ends = [depot] * n_veh

    manager = pywrapcp.RoutingIndexManager(n_nodes, n_veh, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    # custo-arco neutro
    zero_cb = routing.RegisterTransitCallback(lambda *_: 0)
    routing.SetArcCostEvaluatorOfAllVehicles(zero_cb)

    # --- aqui, logo após criar routing e antes de resolver, adicione:
    for i in range(n_srv):
        # pickup em (1 + i), delivery em (1 + n_srv + i)
        p = manager.NodeToIndex(1 + i)
        d = manager.NodeToIndex(1 + n_srv + i)
        routing.AddPickupAndDelivery(p, d)

    # custo-arco neutro
    zero_cb = routing.RegisterTransitCallback(lambda *_: 0)
    routing.SetArcCostEvaluatorOfAllVehicles(zero_cb)

    # ───────────────────────────────────────────────────────
    # 3) Dimensões de capacidade (CEU / LIG / FUR / ROD)
    # ───────────────────────────────────────────────────────
    demand_cbs = create_demand_callbacks(df, manager, routing, depot_indices=[depot])
    add_dimensions_and_constraints(routing, trailers, demand_cbs)

    # ───────────────────────────────────────────────────────
    # 4) Pares pickup-delivery (mesmo veículo, pickup ≺ delivery)
    # ───────────────────────────────────────────────────────
    for i in range(n_srv):
        p = manager.NodeToIndex(1 + i)
        d = manager.NodeToIndex(1 + n_srv + i)
        routing.AddPickupAndDelivery(p, d)

    # ───────────────────────────────────────────────────────
    # 5) Resolve
    # ───────────────────────────────────────────────────────
    search = pywrapcp.DefaultRoutingSearchParameters()
    search.time_limit.seconds = 60
    search.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )

    solution = routing.SolveWithParameters(search)
    if solution is None:
        logger.warning("⚠️ Solver não encontrou solução.")
        return []

    # ───────────────────────────────────────────────────────
    # 6) Extrai rotas
    # ───────────────────────────────────────────────────────
    routes: list[tuple[int, list[int]]] = []
    for v in range(n_veh):
        idx = routing.Start(v)
        path: list[int] = []
        while not routing.IsEnd(idx):
            node = manager.IndexToNode(idx)
            if node != depot:  # ignora o depósito
                path.append(node - 1)  # converte para 0..2*n_srv-1
            idx = solution.Value(routing.NextVar(idx))
        routes.append((v, path))

    # ───────────────────────────────────────────────────────
    # 7) Persiste (km = 0, CEU calculado no persist)
    # ───────────────────────────────────────────────────────
    rota_ids = await persist_routes(
        sess,
        dia,
        df,
        routes,
        trailer_starts=[depot] * n_veh,
        trailers=trailers,
        # dist_matrix=None,  # km = 0
        # city_idx=None,  # não usado
    )
    logger.info("✅ %s rotas persistidas.", len(rota_ids))
    return rota_ids
