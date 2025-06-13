# backend/solver/optimizer/setup_model.py

import logging
from typing import List, Tuple
from ortools.constraint_solver import pywrapcp

from backend.solver.optimizer.city_mapping import (
    get_unique_cities,
    map_city_indices,
    build_distance_matrix,
    map_bases_to_indices,
)

logger = logging.getLogger(__name__)


def pad_dist_matrix(dist_matrix: List[List[int]], target_size: int) -> List[List[int]]:
    """
    Ajusta a matriz de distâncias para o número de índices esperado pelo manager.
    Preenche com penalidades elevadas onde necessário.
    """
    size = len(dist_matrix)
    padded = [[999999 for _ in range(target_size)] for _ in range(target_size)]
    for i in range(size):
        for j in range(size):
            padded[i][j] = dist_matrix[i][j]
    return padded


def create_manager_and_model(
    locations: List[str], starts: List[int], ends: List[int]
) -> Tuple[pywrapcp.RoutingIndexManager, pywrapcp.RoutingModel]:
    """
    Inicializa o manager e o modelo de routing da OR-Tools.
    """
    n_nodes = len(locations)
    n_vehicles = len(starts)

    assert all(0 <= s < n_nodes for s in starts), "⚠️ Índices 'starts' inválidos"
    assert all(0 <= e < n_nodes for e in ends), "⚠️ Índices 'ends' inválidos"
    if len(starts) != len(ends):
        raise ValueError(f"🚨 Número de 'starts' ≠ 'ends': {len(starts)} ≠ {len(ends)}")

    logger.debug(f"🧭 Locais: {n_nodes}, Veículos: {n_vehicles}")
    logger.debug(f"🔁 Starts: {starts}, 🔚 Ends: {ends}")

    manager = pywrapcp.RoutingIndexManager(n_nodes, n_vehicles, starts, ends)
    routing = pywrapcp.RoutingModel(manager)
    return manager, routing


def set_cost_callback(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    dist_matrix: List[List[int]],
):
    """
    Define o callback de custo baseado na matriz de distâncias, com proteções.
    """

    def cost_cb(i, j):
        try:
            from_node = manager.IndexToNode(i)
            to_node = manager.IndexToNode(j)
            return dist_matrix[from_node][to_node]
        except Exception as e:
            logger.error(f"⛔ Erro no cost_cb: i={i}, j={j} -> {e}")
            return 999999

    index = routing.RegisterTransitCallback(cost_cb)
    routing.SetArcCostEvaluatorOfAllVehicles(index)
    logger.debug("✅ Callback de custo registrado")


def setup_routing_model(
    df,
    trailers,
) -> Tuple[
    pywrapcp.RoutingModel, pywrapcp.RoutingIndexManager, List[int], List[List[int]]
]:
    """
    Prepara o modelo de otimização de rotas com a OR-Tools.
    Retorna modelo, manager, índices de base e matriz de distância.
    """
    locations = get_unique_cities(df, trailers)
    print(f"➡️ Cidades únicas utilizadas: {locations}")

    city_index_map = map_city_indices(locations)
    print(f"➡️ city_index_map: {city_index_map}")

    starts, ends = map_bases_to_indices(trailers, city_index_map)
    print(f"➡️ Índices de partida: {starts}")
    print(f"➡️ Índices de chegada: {ends}")

    dist_matrix = build_distance_matrix(locations)
    print(
        f"➡️ Tamanho original do dist_matrix: {len(dist_matrix)}x{len(dist_matrix[0])}"
    )

    manager, routing = create_manager_and_model(locations, starts, ends)
    print(f"➡️ Nº nós no manager: {manager.GetNumberOfNodes()}")
    print(f"➡️ Nº índices no manager: {manager.GetNumberOfIndices()}")

    # 🩹 Ajustar matriz de distância para OR-Tools
    padded_matrix = pad_dist_matrix(dist_matrix, manager.GetNumberOfNodes())
    print(f"➡️ Tamanho padded_matrix: {len(padded_matrix)}x{len(padded_matrix[0])}")

    set_cost_callback(routing, manager, padded_matrix)
    print("✅ Callback de custo de distância definido")

    return routing, manager, starts, padded_matrix
