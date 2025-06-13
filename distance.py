# backend/solver/distance.py

from typing import Dict, Tuple, List
import unicodedata
import logging
from geopy.distance import geodesic  # type: ignore

logger = logging.getLogger(__name__)

# Cache interno de coordenadas: norm_name → (lat, lon)
_COORDS_CACHE: Dict[str, Tuple[float, float]] = {}


def _norm(city: str) -> str:
    """
    Normaliza o nome da cidade:
    - Remove acentos (NFKD)
    - Converte para ASCII
    - Upper case e strip
    """
    return (
        unicodedata.normalize("NFKD", city)
        .encode("ASCII", "ignore")
        .decode()
        .upper()
        .strip()
    )


def register_coords(coords_map: Dict[str, Tuple[float, float]]) -> None:
    """
    Popula o cache de coordenadas.
    coords_map: { nome_original: (lat, lon), ... }
    """
    for city, coord in coords_map.items():
        norm_name = _norm(city)
        _COORDS_CACHE[norm_name] = coord
    logger.info(f"✅ Registradas {len(coords_map)} coordenadas no cache")


def _coords(city: str) -> Tuple[float, float]:
    """
    Lê do cache as coordenadas normalizadas.
    Lança ValueError se não existir no cache.
    """
    norm_name = _norm(city)
    if norm_name not in _COORDS_CACHE:
        raise ValueError(
            f"Coordenadas de cidade desconhecida: '{city}' (normalizada como '{norm_name}')"
        )
    return _COORDS_CACHE[norm_name]


def _distance_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """
    Distância geodésica (geopy) entre dois pares (lat, lon), em km.
    """
    return geodesic(a, b).km  # type: ignore


def build_distance_matrix(locations: List[str]) -> List[List[float]]:
    """
    Constroi matriz de distâncias (float km) apenas para as 'locations' fornecidas.
    Antes de chamar este método, deve ter sido feito:
        register_coords({ cidade: (lat, lon), ... })

    locations: lista de nomes normalizados (ou que _norm converte).
    Retorna: matriz n x n, onde mat[i][j] = km entre locations[i] e locations[j].
    """
    coords = [_coords(city) for city in locations]
    n = len(coords)
    mat: List[List[float]] = [[0.0] * n for _ in range(n)]

    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            mat[i][j] = _distance_km(coords[i], coords[j])

    logger.debug(f"➡️ Distância calculada para {n} locais (matriz {n}x{n})")
    return mat
