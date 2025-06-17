# backend/solver/optimizer/setup_model.py

import logging
from typing import List, Tuple
from ortools.constraint_solver import pywrapcp

from backend.solver.optimizer.city_mapping import (
    get_unique_cities,
    map_city_indices,
    build_city_index_and_matrix,
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
            from_node = manager.IndexToNode(i)
            to_node = manager.IndexToNode(j)
            logger.error(f"⛔ cost_cb failed: i={i} j={j} from={from_node} to={to_node} → {e}")
            return 1  # ⚠️ fallback leve p/ evitar travamento total

    index = routing.RegisterTransitCallback(cost_cb)
    routing.SetArcCostEvaluatorOfAllVehicles(index)
    logger.debug("✅ Callback de custo registrado")


def setup_routing_model(
    df,
    trailers,
    debug=False
) -> Tuple[
    pywrapcp.RoutingModel, pywrapcp.RoutingIndexManager, List[int], List[List[int]]
]:
    print("🔥 setup_routing_model foi chamado")
    locations, city_index_map, dist_matrix = build_city_index_and_matrix(df, trailers)

    logger.info(f"➡️ Cidades únicas utilizadas ({len(locations)}): {locations}")
    if debug:
        logger.debug(f"📍 city_index_map: {city_index_map}")

    # 🚨 Validação da dist_matrix
    for i, row in enumerate(dist_matrix):
        for j, val in enumerate(row):
            if not isinstance(val, int) or val < 0:
                logger.error(f"🚫 dist_matrix[{i}][{j}] inválido: {val}")
                if i < len(locations) and j < len(locations):
                    logger.error(f"↪️ Cidades: {locations[i]} → {locations[j]}")
                raise ValueError(f"Distância inválida em dist_matrix[{i}][{j}] = {val}")

    starts, ends = map_bases_to_indices(trailers, city_index_map)
    logger.debug(f"🚚 Starts: {starts} | Ends: {ends}")
    logger.debug(f"📊 city_index_map: {city_index_map}")
    logger.debug(f"🧮 Total locations: {len(locations)}")
    if debug and dist_matrix:
        logger.debug(f"🧪 Exemplo dist_matrix[0][:5]: {dist_matrix[0][:5]}")

    manager, routing = create_manager_and_model(locations, starts, ends)
    logger.debug(f"🧠 manager.GetNumberOfNodes() = {manager.GetNumberOfNodes()}")
    logger.debug(f"🧠 manager.GetNumberOfIndices() = {manager.GetNumberOfIndices()}")

    padded_matrix = pad_dist_matrix(dist_matrix, manager.GetNumberOfNodes())

    # 🚨 Verificação de integridade da matriz padded_matrix
    for i, row in enumerate(padded_matrix):
        for j, val in enumerate(row):
            if not isinstance(val, int) or val < 0:
                logger.error(f"🚫 Distância inválida em padded_matrix[{i}][{j}] = {val}")
                if i < len(locations) and j < len(locations):
                    logger.error(f"↪️ Cidades: {locations[i]} → {locations[j]}")
                raise ValueError(f"Distância inválida em padded_matrix[{i}][{j}] = {val}")

    # 👇 Prévia segura da matriz se debug ativado
    if debug:
        preview_rows = padded_matrix[:min(5, len(padded_matrix))]
        logger.debug(f"🔍 Preview padded_matrix (máx 5 linhas): {preview_rows}")

    set_cost_callback(routing, manager, padded_matrix)
    logger.info("✅ Callback de custo de distância definido")

    return routing, manager, starts, padded_matrix
