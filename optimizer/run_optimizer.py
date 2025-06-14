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
    # ─── 1) Carrega serviços + trailers ───────────────────────────────────────
    df, trailers, base_map = await prepare_input_dataframe(sess, dia, matricula)
    if df.empty:
        logger.warning("⚠️ Nenhum serviço elegível.")
        return []
    if not trailers:
        logger.warning("⚠️ Nenhum trailer ativo.")
        return []

    # Filtra por categoria, se pedido
    if categoria_filtrada:
        from .trailer_routing import filter_services_by_category

        df = filter_services_by_category(df, categoria_filtrada, base_map)
        if df.empty:
            logger.warning("⚠️ Nenhum serviço após filtro de categoria.")
            return []

    # ─── 2) Configura manager/model “capacidade-only” ────────────────────────
    n_srv = len(df)  # serviços
    n_veh = len(trailers)  # trailers = veículos
    depot = 0  # nó 0 é depósito fictício

    # 1 depósito + n_srv pickups + n_srv deliveries
    n_nodes = 1 + 2 * n_srv
    starts = [depot] * n_veh
    ends = [depot] * n_veh

    manager = pywrapcp.RoutingIndexManager(n_nodes, n_veh, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    # custo-arco neutro (tudo = 0)
    zero_cb = routing.RegisterTransitCallback(lambda i, j: 0)
    routing.SetArcCostEvaluatorOfAllVehicles(zero_cb)

    # ─── 3) Dimensões de capacidade (CEU, LIG, FUR, ROD) ────────────────────
    demand_cbs = create_demand_callbacks(df, manager, routing, depot_indices=[depot])
    add_dimensions_and_constraints(routing, trailers, demand_cbs)

    # ─── 4) Pares pickup-delivery (mesmo veículo + precedência) ─────────────
    # Requer que já exista dimensão “CEU”
    ceu_dim = routing.GetDimensionOrDie("CEU")
    solver = routing.solver()
    for i in range(n_srv):
        # pickup em node (1 + i), delivery em (1 + n_srv + i)
        p_idx = manager.NodeToIndex(1 + i)
        d_idx = manager.NodeToIndex(1 + n_srv + i)
        routing.AddPickupAndDelivery(p_idx, d_idx)
        # mesmo veículo
        solver.Add(routing.VehicleVar(p_idx) == routing.VehicleVar(d_idx))
        # pickup antes do delivery (pela cumul da dimensão CEU)
        solver.Add(ceu_dim.CumulVar(p_idx) <= ceu_dim.CumulVar(d_idx))

    # ─── 5) Parâmetros de pesquisa + resolve ────────────────────────────────
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_params.time_limit.seconds = 60

    solution = routing.SolveWithParameters(search_params)
    if solution is None:
        logger.warning("⚠️ Solver não encontrou solução.")
        return []

    # ─── 6) Extrai rotas do solver ───────────────────────────────────────────
    routes: list[tuple[int, list[int]]] = []
    for v in range(n_veh):
        idx = routing.Start(v)
        path: list[int] = []
        while not routing.IsEnd(idx):
            node = manager.IndexToNode(idx)
            if node != depot:
                # converte de 1..2*n_srv → 0..2*n_srv-1
                path.append(node - 1)
            idx = solution.Value(routing.NextVar(idx))
        if path:
            routes.append((v, path))

    # ─── 7) Persiste resultados ────────────────────────────────────────────
    rota_ids = await persist_routes(
        sess,
        dia,
        df,
        routes,
        trailer_starts=[depot] * n_veh,
        trailers=trailers,
        # km e city_idx não usados nesta versão “capacidade-only”
    )
    logger.info("✅ %d rotas persistidas.", len(rota_ids))
    return rota_ids
