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
    solution,
    n_services: int,
    debug: bool = False,
) -> list[tuple[int, list[int]]]:
    rotas = []
    for v in range(routing.vehicles()):
        idx = routing.Start(v)
        path = []
        while not routing.IsEnd(idx):
            node = manager.IndexToNode(idx)
            # ignora o depósito fictício (0)
            if node != 0:
                # converte de 1..2*n_services para 0..2*n_services-1
                path.append(node - 1)
            idx = solution.Value(routing.NextVar(idx))
        if path:
            rotas.append((v, path))
    return rotas


def norm(texto: str) -> str:
    if not isinstance(texto, str) or not texto.strip():
        return "DESCONHECIDA"

    texto_normalizado = unicodedata.normalize("NFKD", texto)
    ascii_texto = texto_normalizado.encode("ASCII", "ignore").decode()
    texto_maiusculo = ascii_texto.upper().strip()

    # Redundante após normalização, mas mantido para casos específicos
    return (
        texto_maiusculo
        .replace("Á", "A")
        .replace("Ã", "A")
        .replace("É", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ú", "U")
        .replace("Ç", "C")
    )


def haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a_ = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    return 2 * 6371.0 * math.asin(math.sqrt(a_))

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
            try:
                mat[i][j] = int(round(haversine_km(coords_map[locations[i]], coords_map[locations[j]])))
            except KeyError as e:
                logger.warning(f"⚠️ Coordenadas ausentes para {e.args[0]}, usando 0km")
                mat[i][j] = 0
    logger.debug(f"↔️ Matriz {n}×{n} construída")
    return mat
