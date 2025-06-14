# backend/solver/optimizer/run_optimizer.py

from __future__ import annotations
import logging
from datetime import date
from typing import Optional, List, Tuple
import faulthandler
from sqlalchemy.ext.asyncio import AsyncSession
import faulthandler
import ortools

print("== OR-Tools version:", ortools.__version__)
import sysconfig

print(
    "== Python ABI:",
    sysconfig.get_config_var("Py_DEBUG"),
    sysconfig.get_config_var("WITH_PYMALLOC"),
)
# OR-Tools
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

# nossos módulos
from .prepare_input import prepare_input_dataframe
from backend.solver.routing import (
    create_demand_callbacks,
    add_dimensions_and_constraints,
)
from .persist_results import persist_routes

logger = logging.getLogger(__name__)
faulthandler.enable(all_threads=True, file=open("/app/faulthandler.log", "w"))


async def optimize(
    sess: AsyncSession,
    dia: date,
    matricula: Optional[str] = None,
    categoria_filtrada: Optional[List[str]] = None,
) -> List[int]:
    # 1) dados
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

    # 2) parâmetros de VRP “capacidade-only”
    n_srv = len(df)
    n_veh = len(trailers)
    depot = 0

    # cada veículo parte/regressa no nó 0;
    # nós 1..n_srv = pickups; nós (1+n_srv)..(2*n_srv) = deliveries
    n_nodes = 1 + 2 * n_srv
    starts = [depot] * n_veh
    ends = [depot] * n_veh

    # CRIAR MANAGER E MODEL
    manager = pywrapcp.RoutingIndexManager(n_nodes, n_veh, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    # DEBUG: agora o manager existe
    print(f"[DEBUG] n_srv={n_srv}, n_veh={n_veh}, n_nodes={n_nodes}")
    print(
        f"[DEBUG] manager.GetNumberOfNodes() = {manager.GetNumberOfNodes()}, "
        f"manager.GetNumberOfIndices() = {manager.GetNumberOfIndices()}"
    )

    # 3) custo-arco neutro
    def _zero_cost(i: int, j: int) -> int:
        return 0

    cost_cb = routing.RegisterTransitCallback(_zero_cost)
    routing.SetArcCostEvaluatorOfAllVehicles(cost_cb)

    # 4) dimensões CEU/LIG/FUR/ROD
    demand_cbs = create_demand_callbacks(df, manager, routing, depot_indices=[depot])
    add_dimensions_and_constraints(routing, trailers, demand_cbs)

    # 5) pares pickup→delivery (mesmo veículo + precedência em CEU)
    ceu_dim = routing.GetDimensionOrDie("CEU")
    solver = routing.solver()
    for i in range(n_srv):
        p_idx = manager.NodeToIndex(1 + i)
        d_idx = manager.NodeToIndex(1 + n_srv + i)
        print(f"[DEBUG] Adding pickup-delivery pair: p_idx={p_idx}, d_idx={d_idx}")
        assert (
            0 <= p_idx < manager.GetNumberOfIndices()
        ), f"p_idx fora do range: {p_idx}"
        assert (
            0 <= d_idx < manager.GetNumberOfIndices()
        ), f"d_idx fora do range: {d_idx}"
        routing.AddPickupAndDelivery(p_idx, d_idx)
        solver.Add(routing.VehicleVar(p_idx) == routing.VehicleVar(d_idx))
        solver.Add(ceu_dim.CumulVar(p_idx) <= ceu_dim.CumulVar(d_idx))

    # 6) resolver
    search = pywrapcp.DefaultRoutingSearchParameters()
    search.time_limit.seconds = 30
    search.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )

    solution = routing.SolveWithParameters(search)
    if solution is None:
        logger.warning("⚠️ Nenhuma solução viável encontrada.")
        return []

    # 7) extrair rotas
    routes: List[Tuple[int, List[int]]] = []
    for v in range(n_veh):
        idx = routing.Start(v)
        path: List[int] = []
        while not routing.IsEnd(idx):
            node = manager.IndexToNode(idx)
            if node != depot:
                path.append(node - 1)  # de volta a 0..2*n_srv-1
            idx = solution.Value(routing.NextVar(idx))
        if path:
            routes.append((v, path))

    # 8) persistir
    rota_ids = await persist_routes(
        sess,
        dia,
        df,
        routes,
        trailer_starts=starts,  # ignorado pela persist atual
        trailers=trailers,
    )
    logger.info("✅ %d rotas persistidas.", len(rota_ids))
    return rota_ids
