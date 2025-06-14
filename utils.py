# backend\solver\utils.py
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
) -> list[tuple[int, list[int]]]:
    """
    Extrai a lista de tuplos (vehicle_id, [nós visitados]),
    incluindo apenas caminhos que tenham pelo menos um pickup E um delivery.
    """
    rotas: list[tuple[int, list[int]]] = []

    for vehicle_id in range(routing.vehicles()):
        index = routing.Start(vehicle_id)
        path: list[int] = []

        # percorre até ao final do veículo
        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            # só nos interessa se for serviço (pickup ou delivery)
            if node < 2 * n_services:
                path.append(node)
            index = solution.Value(routing.NextVar(index))

        # verifica se tem pickup E delivery
        has_pickup = any(n < n_services for n in path)
        has_delivery = any(n >= n_services for n in path)
        if has_pickup and has_delivery:
            if debug:
                logging.debug(f"→ Veículo {vehicle_id} path raw: {path}")
            # opcional: eliminar duplicações consecutivas
            deduped = [path[0]]
            for nxt in path[1:]:
                if nxt != deduped[-1]:
                    deduped.append(nxt)
            if debug:
                logging.debug(f"→ Veículo {vehicle_id} path deduped: {deduped}")
            rotas.append((vehicle_id, deduped))

    return rotas


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
