# backend/solver/optimizer/run_optimizer.py

from __future__ import annotations
import logging
from datetime import date
from typing import Optional, List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from .prepare_input import prepare_input_dataframe
from .constraints import apply_all_constraints
from .solve_model import solve_with_params
from .persist_results import persist_routes
from backend.solver.utils import extract_routes  # ajusta o import se necessário

logger = logging.getLogger(__name__)


async def optimize(
    sess: AsyncSession,
    dia: date,
    matricula: Optional[str] = None,
    categoria_filtrada: Optional[List[str]] = None,
) -> List[int]:
    # 1) Carrega e prepara dados
    df, trailers, base_map = await prepare_input_dataframe(sess, dia, matricula)
    if df.empty:
        logger.warning("⚠️ Nenhum serviço elegível.")
        return []
    if not trailers:
        logger.warning("⚠️ Nenhum trailer activo.")
        return []

    # filtro opcional por categoria
    if categoria_filtrada:
        from .trailer_routing import filter_services_by_category

        df = filter_services_by_category(df, categoria_filtrada, base_map)
        if df.empty:
            logger.warning("⚠️ Nenhum serviço após filtro de categoria.")
            return []

    # 2) Parâmetros do modelo
    n_srv = len(df)  # serviços = pickups
    n_veh = len(trailers)  # um veículo por trailer
    depot = 0
    # 1 nó de depósito + n_srv pickups + n_srv deliveries
    n_nodes = 1 + 2 * n_srv
    starts = [depot] * n_veh
    ends = [depot] * n_veh

    # 3) Cria manager + modelo
    manager = pywrapcp.RoutingIndexManager(n_nodes, n_veh, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    # custo-arco neutro (todos arcos iguais)
    zero_cb = routing.RegisterTransitCallback(lambda i, j: 0)
    routing.SetArcCostEvaluatorOfAllVehicles(zero_cb)

    # 4) Carrega pesos de constraints (mesma tabela que antes)
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
    rows = q.fetchall()
    if rows:
        weights = {r.cod: float(r.valor) for r in rows}
    else:
        logger.warning("⚠️ Nenhum peso de restrição encontrado — usando padrão.")
        weights = {
            "INTERNO_LOW_PEN": 10.0,
            "PENALIDADE_DIST_KM": 3.0,
            "MAX_DIST_POR_TRAILER": 400.0,
        }

    # 5) Aplica *todas* constraints, incluindo pickup‐delivery
    apply_all_constraints(
        routing=routing,
        manager=manager,
        df=df,
        trailers=trailers,
        n_services=n_srv,
        depot_indices=[depot],
        distance_matrix=None,  # ainda não usamos dist_matrix
        constraint_weights=weights,
        enable_interno=False,
        enable_force_return=False,
        enable_pickup_pairs=True,  # <-- ativa pares pickup‐delivery
    )

    # 6) Parâmetros de busca e resolução
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.time_limit.seconds = 60
    search_params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )

    solution = routing.SolveWithParameters(search_params)
    if solution is None:
        logger.warning("⚠️ Solver não encontrou solução viável.")
        return []

    # 7) Extrai as rotas (pickup + delivery juntos)
    routes = extract_routes(
        routing,
        manager,
        solution,
        n_services=n_srv,
        debug=False,
    )

    # 8) Persiste resultados (km=0, CEU calculado em persist_results)
    rota_ids = await persist_routes(
        sess,
        dia,
        df,
        routes,
        trailer_starts=[depot] * n_veh,
        trailers=trailers,
        # dist_matrix e city_idx não são necessários aqui
    )
    logger.info("✅ %d rotas persistidas.", len(rota_ids))
    return rota_ids
