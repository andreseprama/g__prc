from ortools.constraint_solver import pywrapcp
from typing import List, Tuple, Dict, Any, Union
import logging
import unicodedata
import math
import os
import httpx
from sqlalchemy import insert
from backend.solver.distance import register_coords, _norm


logger = logging.getLogger(__name__)


def extract_routes(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    solution: Any,
    n_services: int = 0,
    debug: bool = False,
) -> List[Tuple[int, List[int]]]:
    """
    Extrai as rotas da solução do VRP OR-Tools.

    Args:
        routing: instância de RoutingModel.
        manager: instância de RoutingIndexManager.
        solution: objeto de solução retornado pelo solver.
        n_services: número de serviços (para filtrar nós de carga/descarga).
        debug: se True, escreve logs intermédios de cada passo.

    Retorna:
        Lista de tuplos (vehicle_id, [lista de nós visitados]).
    """
    result: List[Tuple[int, List[int]]] = []

    for vehicle_id in range(routing.vehicles()):
        index = routing.Start(vehicle_id)
        path: List[int] = []

        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            if debug:
                logger.debug(
                    f"🚚 Trailer {vehicle_id} visitando node {node} (idx {index})"
                )
            # só adiciona nós que correspondem a serviços
            if node < 2 * n_services:
                path.append(node)
            index = solution.Value(routing.NextVar(index))

        # inclui nó de final (caso seja serviço)
        final_node = manager.IndexToNode(index)
        if final_node < 2 * n_services:
            path.append(final_node)

        if len(path) > 1:
            result.append((vehicle_id, path))
            if path:
                logging.debug(f"→ Rota (mesmo que curto) veículo {vehicle_id}: {path}")
                result.append((vehicle_id, path))

    return result


def norm(texto: str) -> str:
    return (
        unicodedata.normalize("NFKD", texto)
        .encode("ASCII", "ignore")
        .decode()
        .upper()
        .strip()
    )


def haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    φ1, φ2 = math.radians(a[0]), math.radians(b[0])
    Δφ, Δλ = math.radians(b[0] - a[0]), math.radians(b[1] - a[1])
    sφ, sλ = math.sin(Δφ / 2), math.sin(Δλ / 2)
    h = sφ * sφ + math.cos(φ1) * math.cos(φ2) * sλ * sλ
    return 2 * 6371.0 * math.asin(math.sqrt(h))


def build_int_distance_matrix(
    locations: List[str],
    coords_map: Dict[str, Tuple[float, float]],
) -> List[List[int]]:
    n = len(locations)
    mat = [[0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            mat[i][j] = int(
                round(haversine_km(coords_map[locations[i]], coords_map[locations[j]]))
            )
    logger.debug(f"↔️ Matriz {n}×{n} construída")
    return mat
