# backend/solver/distance.py

from typing import Dict, Tuple, List
import unicodedata
import logging
from geopy.distance import geodesic  # type: ignore
import pandas as pd 
import csv
import os
from datetime import datetime, date
from typing import Optional

# Cache de cidades inválidas
_INVALID_CITY_LOG: List[Dict[str, str]] = []

logger = logging.getLogger(__name__)

# Cache interno de coordenadas: norm_name → (lat, lon)
_COORDS_CACHE: Dict[str, Tuple[float, float]] = {}


def _norm(texto: Optional[str]) -> str:
    if not isinstance(texto, str) or not texto.strip():
        return "DESCONHECIDA"

    texto_normalizado = unicodedata.normalize("NFKD", texto)
    ascii_texto = texto_normalizado.encode("ASCII", "ignore").decode()
    texto_maiusculo = ascii_texto.upper().strip()

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

def coordenada_real(row, tipo="load"):
    if row[f"{tipo}_is_base"] and pd.notnull(row["scheduled_base"]):
        return row["scheduled_base"]
    return row[f"{tipo}_city"]


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


def get_coords(city: Optional[str], *, service_id: Optional[str] = None, plate: Optional[str] = None) -> Tuple[float, float] | None:
    norm_name = _norm(city)

    if norm_name == "DESCONHECIDA" or norm_name not in _COORDS_CACHE:
        entry = {
            "service_id": service_id or "desconhecido",
            "matricula": plate or "desconhecida",
            "cidade_original": city or "None",
            "cidade_normalizada": norm_name,
        }
        _INVALID_CITY_LOG.append(entry)
        logger.warning(f"❌ Cidade inválida: {entry}")
        return None

    return _COORDS_CACHE[norm_name]


def exportar_cidades_invalidas_csv(
    base_path: str = "/wsl.localhost/Ubuntu-24.04/home/andrecouto/1GESTOW_GESTAO_SERVICOS/backend/solver"
) -> None:
    if not _INVALID_CITY_LOG:
        logger.info("✅ Nenhuma cidade inválida detectada.")
        return

    today_str = date.today().isoformat()
    filename = f"cidades_invalidas_{today_str}.csv"
    full_path = os.path.join(base_path, filename)

    try:
        with open(full_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["service_id", "matricula", "cidade_original", "cidade_normalizada"])
            writer.writeheader()
            writer.writerows(_INVALID_CITY_LOG)

        logger.info(f"📤 CSV de cidades inválidas salvo em: {full_path}")

    except Exception as e:
        logger.error(f"❌ Erro ao salvar CSV de cidades inválidas: {e}")