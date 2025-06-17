from ortools.constraint_solver import routing_enums_pb2, pywrapcp
import logging

def solve_with_params(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    time_limit_sec: int = 120,
    log_search: bool = True,
    first_solution_strategy: routing_enums_pb2.FirstSolutionStrategy.Value = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC,
    local_search_metaheuristic: routing_enums_pb2.LocalSearchMetaheuristic.Value = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH,
) -> pywrapcp.Assignment | None:
    """
    Resolve o modelo com parâmetros configurados.

    Args:
        routing: O modelo de roteamento OR-Tools.
        manager: O gerenciador de índices de roteamento.
        time_limit_sec: Tempo limite para o solver (em segundos).
        log_search: Se True, ativa o log da pesquisa.
        first_solution_strategy: Estratégia de primeira solução.
        local_search_metaheuristic: Metaheurística de pesquisa local.

    Returns:
        A solução encontrada (Assignment) ou None se não houver solução.
    """
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.time_limit.seconds = time_limit_sec
    search_params.log_search = log_search
    search_params.first_solution_strategy = first_solution_strategy
    search_params.local_search_metaheuristic = local_search_metaheuristic

    solution = routing.SolveWithParameters(search_params)

    if solution:
        logging.info("✅ Solução encontrada para o problema.")
    else:
        logging.warning("❌ Nenhuma solução encontrada dentro do limite de tempo.")

    return solution
