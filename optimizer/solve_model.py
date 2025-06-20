from ortools.constraint_solver import routing_enums_pb2, pywrapcp
import logging
import csv
from typing import Any
import pandas as pd 

logger = logging.getLogger(__name__)

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

    logger.debug(f"🔍 Validando modelo: {routing.vehicles()} veículos, {routing.Size()} nós")

    if routing.vehicles() == 0 or routing.Size() == 0:
        logger.critical("❌ Modelo inválido: sem veículos ou nós.")
        return None

    try:
        for i in range(manager.GetNumberOfNodes()):
            _ = manager.IndexToNode(i)
    except Exception as e:
        logger.critical(f"❌ Erro ao validar índices de nodes: {e}")
        return None

    try:
        solution = routing.SolveWithParameters(search_params)
    except Exception as e:
        logger.critical(f"💥 Erro ao executar SolveWithParameters: {e}")
        return None

    if solution:
        logger.info("✅ Solução encontrada para o problema.")
    else:
        logger.warning("❌ Nenhuma solução encontrada dentro do limite de tempo.")

    return solution


def extract_solution(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    solution: pywrapcp.Assignment,
    df: pd.DataFrame,
    df_idx_map: dict[int, int],
    export_csv: bool = False,
    csv_path: str = "rota_extraida.csv"
) -> list[list[int]]:
    """
    Extrai rotas da solução com metadados e exportação opcional em CSV.

    Args:
        routing: Modelo OR-Tools.
        manager: Gerenciador de índices.
        solution: Objeto da solução.
        df: DataFrame original com serviços.
        df_idx_map: Mapeamento entre solver_idx → df_idx.
        export_csv: Exporta para CSV se True.
        csv_path: Caminho do CSV.

    Returns:
        Lista de rotas extraídas.
    """
    if not solution:
        logger.error("❌ Nenhuma solução fornecida.")
        return []

    rotas_extraidas = []
    linhas_csv = []

    try:
        for veiculo_id in range(routing.vehicles()):
            index = routing.Start(veiculo_id)
            rota = []

            ordem = 0
            while not routing.IsEnd(index):
                node_id = manager.IndexToNode(index)
                rota.append(node_id)

                solver_idx = index
                df_idx = df_idx_map.get(solver_idx, None)

                if df_idx is not None and 0 <= df_idx < len(df):
                    row = df.iloc[df_idx]
                    linha = {
                        "veiculo_id": veiculo_id,
                        "ordem": ordem,
                        "node_id": node_id,
                        "id": row.get("id"),
                        "matricula": row.get("matricula"),
                        "cidade": row.get("load_city"),
                        "service_reg": row.get("service_reg"),
                    }
                    linhas_csv.append(linha)

                ordem += 1
                index = solution.Value(routing.NextVar(index))

            # Adiciona nó final (end)
            end_node = manager.IndexToNode(index)
            rota.append(end_node)
            rotas_extraidas.append(rota)

            linhas_csv.append({
                "veiculo_id": veiculo_id,
                "ordem": ordem,
                "node_id": end_node,
                "id": None,
                "matricula": None,
                "cidade": "END",
                "service_reg": None,
            })

            logger.debug(f"🚛 Veículo {veiculo_id} → Rota: {rota}")

        if export_csv:
            try:
                with open(csv_path, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=linhas_csv[0].keys())
                    writer.writeheader()
                    writer.writerows(linhas_csv)
                logger.info(f"📄 CSV exportado: {csv_path}")
            except Exception as e:
                logger.error(f"❌ Erro ao exportar rota CSV: {e}")

    except Exception as e:
        logger.exception(f"💥 Falha ao extrair rotas: {e}")
        return []

    return rotas_extraidas