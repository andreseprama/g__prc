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
    Extrai (vehicle_id, [service_indices]) de cada veículo, incluindo apenas
    rotas que contenham pelo menos um pickup e uma delivery.
    service_index é 0..n_services-1 para pickups, n_services..2*n_services-1 para deliveries.
    """
    rotas: list[tuple[int, list[int]]] = []
    # descobre qual o node do "depósito"
    depot_node = manager.IndexToNode(routing.Start(0))

    for vehicle_id in range(routing.vehicles()):
        index = routing.Start(vehicle_id)
        raw: list[int] = []

        # percorre toda a rota do veículo
        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            # ignora depósito e nodes fora do range de serviços
            if node != depot_node and 1 <= node < 1 + 2 * n_services:
                raw.append(node)
            index = solution.Value(routing.NextVar(index))

        # converte de [1..2*n_services] → [0..2*n_services-1]
        path = [node - 1 for node in raw]

        # verifica se há pelo menos um pickup e uma delivery
        has_pickup = any(s < n_services for s in path)
        has_delivery = any(s >= n_services for s in path)
        if not (has_pickup and has_delivery):
            continue

        if debug:
            logging.debug(f"Veículo {vehicle_id} raw path nodes: {raw}")
            logging.debug(f"Veículo {vehicle_id} mapped path:    {path}")

        # remove duplicações consecutivas
        deduped: list[int] = []
        prev = None
        for s in path:
            if s != prev:
                deduped.append(s)
                prev = s

        if debug:
            logging.debug(f"Veículo {vehicle_id} deduped path:   {deduped}")

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
