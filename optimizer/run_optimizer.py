# backend/solver/optimizer/run_optimizer.py

from __future__ import annotations
import logging
from datetime import date
from typing import Optional, List, Tuple
import faulthandler

from sqlalchemy.ext.asyncio import AsyncSession
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from .prepare_input import prepare_input_dataframe
from backend.solver.routing import (
    create_demand_callbacks,
    add_dimensions_and_constraints,
)
from .persist_results import persist_routes

faulthandler.enable()
logger = logging.getLogger(__name__)


async def optimize(
    sess: AsyncSession,
    dia: date,
    matricula: Optional[str] = None,
    categoria_filtrada: Optional[List[str]] = None,
    debug: bool = False,
    safe: bool = False,
) -> List[int]:
    df, trailers, base_map = await prepare_input_dataframe(sess, dia, matricula)
    if df.empty:
        logger.warning("⚠️ Nenhum serviço elegível para %s", dia)
        return []
    if not trailers:
        logger.warning("⚠️ Nenhum trailer activo.")
        return []

    if categoria_filtrada:
        from .trailer_routing import filter_services_by_category
        df = filter_services_by_category(df, categoria_filtrada, base_map)
        if df.empty:
            logger.warning("⚠️ Nenhum serviço após filtro de categoria.")
            return []

    if safe:
        df = df.head(3)
        trailers = trailers[:3]

    n_srv = len(df)
    n_veh = len(trailers)
    depot = 0
    n_nodes = 1 + 2 * n_srv
    starts = [depot] * n_veh
    ends = [depot] * n_veh

    manager = pywrapcp.RoutingIndexManager(n_nodes, n_veh, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    print(f"[DEBUG] n_srv={n_srv}, n_veh={n_veh}, n_nodes={n_nodes}")
    print(f"[DEBUG] manager.GetNumberOfNodes() = {manager.GetNumberOfNodes()}, "
          f"manager.GetNumberOfIndices() = {manager.GetNumberOfIndices()}")

    def _zero_cost(i: int, j: int) -> int:
        return 0

    cost_cb = routing.RegisterTransitCallback(_zero_cost)
    assert cost_cb >= 0, "Erro ao registrar transit callback"
    routing.SetArcCostEvaluatorOfAllVehicles(cost_cb)

    demand_cbs = create_demand_callbacks(df, manager, routing, depot_indices=[depot])
    add_dimensions_and_constraints(routing, trailers, demand_cbs)

    ceu_dim = routing.GetDimensionOrDie("CEU")
    solver = routing.solver()

    for i in range(n_srv):
        p_idx = manager.NodeToIndex(1 + i)
        d_idx = manager.NodeToIndex(1 + n_srv + i)
        print(f"[DEBUG] Adding pickup-delivery pair: p_idx={p_idx}, d_idx={d_idx}")
        if routing.IsStart(p_idx) or routing.IsEnd(p_idx):
            print(f"⚠️ p_idx inválido: {p_idx}")
            continue
        if routing.IsStart(d_idx) or routing.IsEnd(d_idx):
            print(f"⚠️ d_idx inválido: {d_idx}")
            continue
        assert 0 <= p_idx < manager.GetNumberOfIndices(), f"p_idx fora do range: {p_idx}"
        assert 0 <= d_idx < manager.GetNumberOfIndices(), f"d_idx fora do range: {d_idx}"
        routing.AddPickupAndDelivery(p_idx, d_idx)
        solver.Add(routing.VehicleVar(p_idx) == routing.VehicleVar(d_idx))
        solver.Add(ceu_dim.CumulVar(p_idx) <= ceu_dim.CumulVar(d_idx))

    search = pywrapcp.DefaultRoutingSearchParameters()
    search.time_limit.seconds = 30
    search.log_search = debug
    search.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.SAVINGS
    search.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.AUTOMATIC

    solution = routing.SolveWithParameters(search)
    if solution is None:
        logger.warning("❌ Nenhuma solução encontrada.")
        if debug:
            with open("model_debug_info.txt", "w") as f:
                f.write(f"[INFO] No solution. n_srv={n_srv}, n_veh={n_veh}, constraints=approx {solver.Constraints()}\n")
        return []

    if debug:
        logger.info("✅ Solver terminou em %d ms com %d nós explorados", solver.WallTime(), solver.Branches())

    routes: List[Tuple[int, List[int]]] = []
    for v in range(n_veh):
        idx = routing.Start(v)
        path: List[int] = []
        while not routing.IsEnd(idx):
            node = manager.IndexToNode(idx)
            if node != depot:
                path.append(node - 1)
            idx = solution.Value(routing.NextVar(idx))
        if path:
            routes.append((v, path))

    rota_ids = await persist_routes(sess, dia, df, routes, trailer_starts=starts, trailers=trailers)
    logger.info("✅ %d rotas persistidas.", len(rota_ids))
    return rota_ids