from ortools.constraint_solver import routing_enums_pb2, pywrapcp
import logging

def solve_with_params(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    time_limit_sec: int = 120,
    log_search: bool = True,
    first_solution_strategy: str = "cheapest",
    local_search_metaheuristic: str = "guided",
) -> pywrapcp.Assignment | None:
    """
    Resolve o modelo com parâmetros configurados.

    Args:
        routing: O modelo de roteamento OR-Tools.
        manager: O gerenciador de índices de roteamento.
        time_limit_sec: Tempo limite para o solver (em segundos).
        log_search: Se True, ativa o log da pesquisa.
        first_solution_strategy: Estratégia de primeira solução ("cheapest", "savings", "parallel", "automatic").
        local_search_metaheuristic: Metaheurística de pesquisa local ("guided", "tabu", "greedy", "automatic").

    Returns:
        A solução encontrada (Assignment) ou None se não houver solução.
    """
    strategy_map = {
        "cheapest": routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC,
        "savings": routing_enums_pb2.FirstSolutionStrategy.SAVINGS,
        "parallel": routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION,
        "automatic": routing_enums_pb2.FirstSolutionStrategy.AUTOMATIC,
    }

    metaheuristic_map = {
        "guided": routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH,
        "tabu": routing_enums_pb2.LocalSearchMetaheuristic.TABU_SEARCH,
        "greedy": routing_enums_pb2.LocalSearchMetaheuristic.GREEDY_DESCENT,
        "automatic": routing_enums_pb2.LocalSearchMetaheuristic.AUTOMATIC,
    }

    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.time_limit.seconds = time_limit_sec
    search_params.log_search = log_search
    search_params.first_solution_strategy = strategy_map.get(
        first_solution_strategy.lower(), routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_params.local_search_metaheuristic = metaheuristic_map.get(
        local_search_metaheuristic.lower(), routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    
    # Proteção antes de Solve
    logging.debug(f"🔍 Validando modelo: {routing.vehicles()} veículos, {manager.GetNumberOfNodes()} nós")
    
    if routing.vehicles() == 0 or manager.GetNumberOfNodes() == 0:
        logging.critical("❌ Modelo inválido: sem veículos ou nós.")
        return None

    try:
        for i in range(manager.GetNumberOfNodes()):
            manager.IndexToNode(i)  # Valida se todos os índices são mapeáveis
    except Exception as e:
        logging.critical(f"❌ Erro ao validar índices de nodes: {e}")
        return None

    solution = routing.SolveWithParameters(search_params)

    if solution:
        logging.info("✅ Solução encontrada para o problema.")
    else:
        logging.warning("❌ Nenhuma solução encontrada dentro do limite de tempo.")

    return solution
