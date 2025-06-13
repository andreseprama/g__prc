from datetime import date
from typing import Optional, List, Dict
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from ortools.constraint_solver import pywrapcp  # OR-Tools

from .prepare_input import prepare_input_dataframe
from .constraints import apply_all_constraints
from .solve_model import solve_with_params
from .persist_results import persist_routes

logger = logging.getLogger(__name__)


async def optimize(
    sess: AsyncSession,
    dia: date,
    matricula: Optional[str] = None,
    categoria_filtrada: Optional[List[str]] = None,
) -> List[int]:
    # ──────────────────────────────────────────────────────────
    # 1) lê serviços + trailers
    # ──────────────────────────────────────────────────────────
    df, trailers, base_map = await prepare_input_dataframe(sess, dia, matricula)
    if df.empty:
        logger.warning("⚠️ Nenhum serviço elegível.")
        return []
    if not trailers:
        logger.warning("⚠️ Nenhum trailer activo.")
        return []

    # opcional: filtrar por categoria
    if categoria_filtrada:
        from .trailer_routing import filter_services_by_category

        df = filter_services_by_category(df, categoria_filtrada, base_map)
        if df.empty:
            logger.warning("⚠️ Nenhum serviço após filtro.")
            return []

    # ──────────────────────────────────────────────────────────
    # 2) modelo apenas-capacidade  (custo-arco = 0)
    # ──────────────────────────────────────────────────────────
    n_srv = len(df)  # 1 nó por serviço
    n_veh = len(trailers)  # 1 veículo por trailer

    # cada veículo tem um “depósito” fictício no nó 0
    starts = [0] * n_veh
    ends = [0] * n_veh

    manager = pywrapcp.RoutingIndexManager(n_srv + 1, n_veh, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    dummy_cb = routing.RegisterTransitCallback(lambda i, j: 0)
    routing.SetArcCostEvaluatorOfAllVehicles(dummy_cb)

    # ──────────────────────────────────────────────────────────
    # 3) constraints (apenas capacidade)
    # ──────────────────────────────────────────────────────────
    # pesos não são usados mas continua a carregar por compat.
    q = await sess.execute(
        text(
            """
            SELECT cd.cod, cw.valor
              FROM constraint_weight cw
              JOIN constraint_def cd ON cd.id = cw.def_id
             WHERE cw.versao = (SELECT MAX(versao) FROM constraint_weight)
        """
        )
    )
    weights = {r.cod: float(r.valor) for r in q.fetchall()}

    apply_all_constraints(
        routing=routing,
        manager=manager,
        df=df,
        trailers=trailers,
        n_services=n_srv,
        depot_indices=[0],  # o nó-0 é o “depósito”
        distance_matrix=None,  # ignorado
        constraint_weights=weights,
        enable_interno=False,
        enable_force_return=False,
        enable_pickup_pairs=False,
    )

    # ──────────────────────────────────────────────────────────
    # 4) solve
    # ──────────────────────────────────────────────────────────
    solution = solve_with_params(routing, manager)
    if solution is None:
        logger.warning("⚠️ Solver não encontrou solução.")
        return []

    # ──────────────────────────────────────────────────────────
    # 5) persistir
    # ──────────────────────────────────────────────────────────
    routes = [
        (v, []) for v in range(routing.vehicles())  # OR-Tools vai preencher logo abaixo
    ]
    for v in range(routing.vehicles()):
        idx = routing.Start(v)
        while not routing.IsEnd(idx):
            node = manager.IndexToNode(idx)
            if node != 0:  # ignora o depósito fictício
                routes[v][1].append(node - 1)  # serviços começam em 0
            idx = solution.Value(routing.NextVar(idx))

    rota_ids = await persist_routes(
        sess,
        dia,
        df,
        routes,
        trailer_starts=[0] * n_veh,  # depósito=0
        trailers=trailers,
        # dist_matrix=None,  # km = 0
    )
    logger.info("✅ %s rotas persistidas.", len(rota_ids))
    return rota_ids
