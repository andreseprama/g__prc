# backend/solver/optimizer/setup_model.py

import logging
from typing import List, Tuple, Dict
from ortools.constraint_solver import pywrapcp
import pandas as pd

from backend.solver.optimizer.city_mapping import (
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
    Define o callback de custo baseado na matriz de distâncias, com proteções adicionais
    para evitar segmentation fault por acessos fora da matriz.
    """

    DEFAULT_PENALTY = 999_999

    def cost_cb(i: int, j: int) -> int:
        from_node, to_node = -1, -1  # Inicializa como inválidos para logging seguro
        try:
            if not (0 <= i < manager.GetNumberOfIndices()) or not (0 <= j < manager.GetNumberOfIndices()):
                logger.warning(f"⚠️ Índice i={i} ou j={j} fora de range válido do manager.")
                return DEFAULT_PENALTY

            from_node = manager.IndexToNode(i)
            to_node = manager.IndexToNode(j)

            if not (0 <= from_node < len(dist_matrix)) or not (0 <= to_node < len(dist_matrix[from_node])):
                logger.warning(f"⚠️ Nós fora da matriz: from_node={from_node}, to_node={to_node}")
                return DEFAULT_PENALTY

            custo = dist_matrix[from_node][to_node]

            if not isinstance(custo, int):
                logger.error(f"🚫 Custo inválido (não-int) entre from_node={from_node} → to_node={to_node}: {custo}")
                return DEFAULT_PENALTY

            logger.debug(f"↪️ Custo entre from_node={from_node} → to_node={to_node} = {custo}")
            return custo

        except Exception as e:
            logger.error(f"⛔ cost_cb erro: i={i} j={j} from={from_node} to={to_node} → {e}")
            return DEFAULT_PENALTY

    index = routing.RegisterTransitCallback(cost_cb)
    routing.SetArcCostEvaluatorOfAllVehicles(index)
    logger.debug("✅ Callback de custo registrado com proteção extra")





def setup_routing_model(
    df: pd.DataFrame,
    trailers: List[dict],
    debug=False
) -> Tuple[
    pywrapcp.RoutingModel,
    pywrapcp.RoutingIndexManager,
    List[int],
    List[List[int]],
    Dict[int, int]
]:
    """
    Setup do modelo de roteamento com OR-Tools.

    Args:
        df (pd.DataFrame): DataFrame contendo as entregas.
        trailers (List[dict]): Lista de dicionários com informações dos trailers.
        debug (bool): Flag para logs detalhados.

    Returns:
        Tuple: routing, manager, starts, padded_matrix, df_idx_map
    """
    print("\U0001F525 setup_routing_model foi chamado")

    locations, city_index_map, dist_matrix = build_city_index_and_matrix(df, trailers)

    logger.info(f"➞ Cidades únicas utilizadas ({len(locations)}): {locations}")
    if debug:
        logger.debug(f"📍 city_index_map: {city_index_map}")

    for i, row in enumerate(dist_matrix):
        for j, val in enumerate(row):
            if not isinstance(val, int) or val < 0:
                logger.error(f"🚫 dist_matrix[{i}][{j}] inválido: {val}")
                if i < len(locations) and j < len(locations):
                    logger.error(f"↪ Cidades: {locations[i]} → {locations[j]}")
                raise ValueError(f"Distância inválida em dist_matrix[{i}][{j}] = {val}")

    starts, ends = map_bases_to_indices(trailers, city_index_map)

    if not starts or not ends:
        raise ValueError("❌ Não foi possível mapear bases de trailers para índices de cidade válidos.")

    logger.debug(f"🚚 Starts: {starts} | Ends: {ends}")
    logger.debug(f"📊 city_index_map: {city_index_map}")
    logger.debug(f"🧼 Total locations: {len(locations)}")
    if debug and dist_matrix:
        logger.debug(f"🕟️ Exemplo dist_matrix[0][:5]: {dist_matrix[0][:5]}")

    if not locations:
        raise ValueError("Lista de 'locations' está vazia — verifique entradas do DataFrame.")

    manager, routing = create_manager_and_model(locations, starts, ends)

    padded_matrix = pad_dist_matrix(dist_matrix, manager.GetNumberOfNodes())

    for i, row in enumerate(padded_matrix):
        for j, val in enumerate(row):
            if not isinstance(val, int) or val < 0:
                logger.error(f"🚫 Distância inválida em padded_matrix[{i}][{j}] = {val}")
                if i < len(locations) and j < len(locations):
                    logger.error(f"↪ Cidades: {locations[i]} → {locations[j]}")
                raise ValueError(f"Distância inválida em padded_matrix[{i}][{j}] = {val}")

    if debug:
        preview_rows = padded_matrix[:min(5, len(padded_matrix))]
        logger.debug(f"🔍 Preview padded_matrix (máx 5 linhas): {preview_rows}")

    set_cost_callback(routing, manager, padded_matrix)
    logger.info("✅ Callback de custo de distância definido")

    df = df.reset_index(drop=True)
    df_idx_map = {}
    max_valid_node = len(df) - 1
    for node in range(manager.GetNumberOfNodes()):
        if node > max_valid_node:
            continue
        try:
            solver_idx = manager.NodeToIndex(node)
            df_idx_map[solver_idx] = node
        except Exception as e:
            logger.error(f"❌ Erro ao mapear node={node} → solver_idx: {e}")

    if debug:
        for solver_idx, df_idx in df_idx_map.items():
            if not (0 <= df_idx < len(df)):
                logger.error(f"❌ Índice inválido: df_idx={df_idx} fora do range (df tem {len(df)} linhas)")
                continue
            row = df.iloc[df_idx]
            logger.debug(f"🔗 Solver node {solver_idx} → df_idx {df_idx} → ID={row['id']}, matrícula={row.get('matricula')}, cidade={row.get('load_city')}")

    return routing, manager, starts, padded_matrix, df_idx_map
