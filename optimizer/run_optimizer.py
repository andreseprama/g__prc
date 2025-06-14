from __future__ import annotations
import logging
from datetime import date
from typing import Optional, List

from sqlalchemy.ext.asyncio import AsyncSession
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from .prepare_input import prepare_input_dataframe
from .constraints import apply_all_constraints
from .persist_results import persist_routes

logger = logging.getLogger(__name__)


async def optimize(
    sess: AsyncSession,
    dia: date,
    matricula: Optional[str] = None,
    categoria_filtrada: Optional[List[str]] = None,
) -> List[int]:
    # 1) input
    df, trailers, base_map = await prepare_input_dataframe(sess, dia, matricula)
    if df.empty or not trailers:
        return []

    # 2) modelo capacity-only com pickup+delivery
    n_srv = len(df)
    n_veh = len(trailers)
    depot = 0
    n_nodes = 1 + 2 * n_srv
    starts = [depot] * n_veh
    ends = [depot] * n_veh

    manager = pywrapcp.RoutingIndexManager(n_nodes, n_veh, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    # arco neutro
    zero_cb = routing.RegisterTransitCallback(lambda *_: 0)
    routing.SetArcCostEvaluatorOfAllVehicles(zero_cb)

    # aplica constraints de capacidade + pickup-delivery
    apply_all_constraints(
        routing=routing,
        manager=manager,
        df=df,
        trailers=trailers,
        n_services=n_srv,
        depot_indices=[depot],
        distance_matrix=None,
        constraint_weights={},
        enable_pickup_pairs=True,
    )

    # 3) resolve
    search = pywrapcp.DefaultRoutingSearchParameters()
    search.time_limit.seconds = 60
    search.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    solution = routing.SolveWithParameters(search)
    if solution is None:
        return []

    # 4) extrai
    routes: list[tuple[int, list[int]]] = []
    for v in range(n_veh):
        idx = routing.Start(v)
        path: list[int] = []
        while not routing.IsEnd(idx):
            node = manager.IndexToNode(idx)
            if node != depot:
                path.append(node - 1)
            idx = solution.Value(routing.NextVar(idx))
        routes.append((v, path))

    # 5) persiste
    rota_ids = await persist_routes(sess, dia, df, routes, [depot] * n_veh, trailers)
    logger.info("✅ %s rotas persistidas.", len(rota_ids))
    return rota_ids
