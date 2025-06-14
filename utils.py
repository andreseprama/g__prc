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
) -> List[Tuple[int, List[int]]]:
    """
    Extrai para cada veículo (vehicle_id, [lista de nós visitados]),
    incluindo pickups (0..n_services-1) e deliveries (n_services..2*n_services-1).
    Retorna apenas rotas não vazias, sem duplicações.
    """
    routes: List[Tuple[int, List[int]]] = []

    for vehicle_id in range(routing.vehicles()):
        index = routing.Start(vehicle_id)
        path: List[int] = []

        # percorre até o End
        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            # só adiciona nós de serviço
            if 0 <= node < 2 * n_services:
                path.append(node)
            index = solution.Value(routing.NextVar(index))

        # opcional: imprimir para debug
        if debug:
            print(f"[DEBUG] Vehicle {vehicle_id} raw path: {path}")

        # só guarda se houver ao menos um pickup E ao menos um delivery
        has_pickup = any(n < n_services for n in path)
        has_delivery = any(n >= n_services for n in path)
        if has_pickup or has_delivery:
            # remove possíveis duplicações de nó contíguo (caso existam)
            deduped = [path[0]]
            for n in path[1:]:
                if n != deduped[-1]:
                    deduped.append(n)
            routes.append((vehicle_id, deduped))

            if debug:
                print(f"[DEBUG] Vehicle {vehicle_id} cleaned path: {deduped}")

    return routes


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
