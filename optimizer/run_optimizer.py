from __future__ import annotations
import logging
from datetime import date
from typing import Optional, List

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
    # 1) load data
    df, trailers, base_map = await prepare_input_dataframe(sess, dia, matricula)
    if df.empty or not trailers:
        logger.warning("⚠️ Nada para otimizar.")
        return []

    # 2) build a “capacity-only” model
    n_srv = len(df)
    n_veh = len(trailers)
    depot = 0

    # 1 depot + n_srv pickups + n_srv deliveries
    n_nodes = 1 + 2 * n_srv
    starts = [depot] * n_veh
    ends = [depot] * n_veh

    manager = pywrapcp.RoutingIndexManager(n_nodes, n_veh, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    # neutral arc‐cost
    zero_cb = routing.RegisterTransitCallback(lambda i, j: 0)
    routing.SetArcCostEvaluatorOfAllVehicles(zero_cb)

    # 3) capacity dimensions
    demand_cbs = create_demand_callbacks(df, manager, routing, depot_indices=[depot])
    add_dimensions_and_constraints(routing, trailers, demand_cbs)

    # 4) pickup-delivery pairing — *only once*, *after* CEU dimension exists
    ceu_dim = routing.GetDimensionOrDie("CEU")
    solver = routing.solver()
    for i in range(n_srv):
        # pickup node ID = 1 + i
        # delivery node ID = 1 + n_srv + i
        p = manager.NodeToIndex(1 + i)
        d = manager.NodeToIndex(1 + n_srv + i)
        routing.AddPickupAndDelivery(p, d)
        solver.Add(routing.VehicleVar(p) == routing.VehicleVar(d))
        solver.Add(ceu_dim.CumulVar(p) <= ceu_dim.CumulVar(d))

    # 5) solve
    search = pywrapcp.DefaultRoutingSearchParameters()
    search.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search.time_limit.seconds = 30

    sol = routing.SolveWithParameters(search)
    if sol is None:
        logger.warning("⚠️ Sem solução.")
        return []

    # 6) extract and log
    routes: list[tuple[int, list[int]]] = []
    for v in range(n_veh):
        idx = routing.Start(v)
        path = []
        while not routing.IsEnd(idx):
            node = manager.IndexToNode(idx)
            if node != depot:
                path.append(node - 1)  # normalize back to 0..2*n_srv-1
            idx = sol.Value(routing.NextVar(idx))
        if path:
            logger.debug(f"→ Vehicle {v} raw path: {path}")
            routes.append((v, path))

    # 7) persist
    rota_ids = await persist_routes(sess, dia, df, routes, [depot] * n_veh, trailers)
    logger.info(f"✅ {len(rota_ids)} rotas gravadas.")
    return rota_ids
