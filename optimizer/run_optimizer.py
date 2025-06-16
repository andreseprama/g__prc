# backend/solver/optimizer/run_optimizer.py

from __future__ import annotations
import logging
from datetime import date
from typing import Optional, List, Tuple, Union
import faulthandler
import pandas as pd

from sqlalchemy.ext.asyncio import AsyncSession
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
from backend.solver.optimizer.rules import flag_return_and_base_fields
from .prepare_input import prepare_input_dataframe
from backend.solver.routing import (
    create_demand_callbacks,
    add_dimensions_and_constraints,
)
from .persist_results import persist_routes
from .subset_selection import selecionar_servicos_e_trailers_compatíveis

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


async def optimize(
    sess: AsyncSession,
    dia: date,
    registry_trailer: Optional[str] = None,
    categoria_filtrada: Optional[List[str]] = None,
    debug: bool = False,
    safe: bool = False,
    max_voltas: int = 10,
) -> Union[List[int], Tuple[List[int], pd.DataFrame]]:
    df, trailers, base_map = await prepare_input_dataframe(sess, dia, registry_trailer)

    # Validação de consistência do DataFrame e trailers
    expected_cols = {"ceu_int", "vehicle_category_name"}
    missing_cols = expected_cols - set(df.columns)
    if missing_cols:
        logger.error("❌ Colunas ausentes no DataFrame: %s", missing_cols)
        return []

    if not isinstance(trailers, list) or not all(isinstance(t, dict) for t in trailers):
        logger.error("❌ Formato inválido para trailers. Esperado: List[dict].")
        return []

    df = flag_return_and_base_fields(df, base_map)
    logger.debug("🔎 Serviços: %d", len(df))

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

    # if safe:
    #     df = df.head(3)
    #     trailers = trailers[:3]

    rota_ids_total = []
    rodada = 1
    df_restante = df

    while not df_restante.empty and trailers and rodada <= max_voltas:
        logger.info("🔁 Rodada %d: %d serviços pendentes", rodada, len(df_restante))
        df_usado, df_restante, trailers_usados = selecionar_servicos_e_trailers_compatíveis(df_restante, trailers)

        if df_usado.empty or not trailers_usados:
            logger.warning("⚠️ Nenhuma combinação viável encontrada na rodada %d.", rodada)
            break

        trailers_restantes = [t for t in trailers if t not in trailers_usados]
        trailers = trailers_restantes

        n_srv = len(df)
        n_veh = len(trailers)
        depot = 0
        n_nodes = 1 + 2 * n_srv
        starts = [depot] * n_veh
        ends = [depot] * n_veh

        manager = pywrapcp.RoutingIndexManager(n_nodes, n_veh, starts, ends)
        routing = pywrapcp.RoutingModel(manager)

        def _zero_cost(i: int, j: int) -> int:
            return 0

        cost_cb = routing.RegisterTransitCallback(_zero_cost)
        routing.SetArcCostEvaluatorOfAllVehicles(cost_cb)

        cb_indices, demand_fns = create_demand_callbacks(df, manager, routing, depot_indices=[depot])
        ceu_caps = _get_ceu_capacities(trailers)

        if debug:
            logger.warning("🧪 Verificação dos demand callbacks (ceu, lig, fur, rod):")
            for kind, fn in demand_fns.items():
                for idx in range(manager.GetNumberOfIndices()):
                    try:
                        val = fn(idx)
                        node = manager.IndexToNode(idx)
                        logger.warning("🧪 %s → idx=%d, node=%d, demand=%s", kind.upper(), idx, node, val)
                    except Exception as e:
                        logger.error("⛔ Callback %s falhou para idx=%d: %s", kind, idx, e)

        routing.AddDimensionWithVehicleCapacity(cb_indices["ceu"], 0, ceu_caps, True, "CEU")
        ceu_dim = routing.GetDimensionOrDie("CEU")
        ceu_dim.SetGlobalSpanCostCoefficient(10000)

        solver = routing.solver()

        for i in range(n_srv):
            p_idx = manager.NodeToIndex(1 + i)
            d_idx = manager.NodeToIndex(1 + n_srv + i)
            ceu_val = int(df.ceu_int.iat[i])
            if routing.IsStart(p_idx) or routing.IsEnd(p_idx):
                continue
            if routing.IsStart(d_idx) or routing.IsEnd(d_idx):
                continue
            logger.warning("📍 DEBUG pickup=%s delivery=%s → node(p)=%d node(d)=%d", p_idx, d_idx,
                           manager.IndexToNode(p_idx), manager.IndexToNode(d_idx))
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
            logger.warning("❌ Nenhuma solução encontrada na rodada %d.", rodada)
            if debug:
                with open("model_debug_info.txt", "w") as f:
                    f.write(f"[INFO] No solution. n_srv={n_srv}, n_veh={n_veh}, constraints=approx {solver.Constraints()}\n")
            break

        if debug:
            logger.info("✅ Solver terminou em %d ms com %d nós explorados", solver.WallTime(), solver.Branches())

        routes: List[Tuple[int, List[int]]] = []
        for v in range(n_veh):
            idx = routing.Start(v)
            path: List[int] = []
            while not routing.IsEnd(idx):
                if idx < 0 or idx >= manager.GetNumberOfIndices():
                    logger.warning("⚠️ Índice inválido no path do veículo %d: idx=%s", v, idx)
                    break
                try:
                    node = manager.IndexToNode(idx)
                except OverflowError as e:
                    logger.error("⛔ Overflow em IndexToNode(idx=%s): %s", idx, e)
                    break
                except Exception as e:
                    logger.error("⛔ Erro inesperado em IndexToNode(idx=%s): %s", idx, e)
                    break

                if node != depot:
                    path.append(node - 1)
                idx = solution.Value(routing.NextVar(idx))

            if path:
                logger.debug("🚚 Veículo %d assigned to serviços: %s", v, path)
                logger.debug("     CEU total: %s", sum(df.ceu_int.iat[n % n_srv] for n in path if n < n_srv))
                routes.append((v, path))

        rota_ids = await persist_routes(sess, dia, df, routes, trailer_starts=starts, trailers=trailers)
        logger.info("✅ Rodada %d: %d rotas persistidas.", rodada, len(rota_ids))
        rota_ids_total.extend(rota_ids)

        rodada += 1

    if not rota_ids_total:
        logger.warning("❌ Nenhuma rota gerada após todas as rodadas.")
        return []

    return rota_ids_total if not debug else (rota_ids_total, df)