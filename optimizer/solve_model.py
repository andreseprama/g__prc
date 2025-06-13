# backend\solver\optimizer\solve_model.py
from ortools.constraint_solver import routing_enums_pb2, pywrapcp
import logging


def solve_with_params(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    time_limit_sec: int = 120,
    log_search: bool = True,
) -> pywrapcp.Assignment | None:
    """
    Resolve o modelo com parâmetros configurados.
    """
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.time_limit.seconds = time_limit_sec
    search_params.log_search = log_search
    search_params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )

    solution = routing.SolveWithParameters(search_params)

    if solution:
        logging.info("✅ Solução encontrada para o problema.")
    else:
        logging.warning("❌ Nenhuma solução encontrada dentro do limite de tempo.")

    return solution
