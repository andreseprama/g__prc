# backend/solver/optimizer/city_mapping.py

from typing import List, Tuple, Dict, Any
import logging

from backend.solver.utils import norm
from backend.solver.distance import build_distance_matrix as _build_distance_matrix

logger = logging.getLogger(__name__)


def get_unique_cities(df, trailers: List[Dict[str, Any]]) -> List[str]:
    """
    Retorna uma lista de cidades únicas normalizadas a partir do DataFrame (load/unload)
    e das cidades base dos trailers.
    """
    # 1) Extrai as cidades de carregamento e descarga
    load_cities = df["load_city"].dropna().astype(str).tolist()
    unload_cities = df["unload_city"].dropna().astype(str).tolist()

    # 2) Extrai as cidades base dos trailers
    base_cities = [
        t.get("base_city", "").strip() for t in trailers if t.get("base_city")
    ]

    # 3) Normaliza todos os nomes
    all_cities = load_cities + unload_cities + base_cities
    normalized = [norm(city) for city in all_cities if city]

    # 4) Remove duplicados mantendo ordem de primeira aparição
    seen = set()
    unique_cities: List[str] = []
    for city in normalized:
        if city not in seen:
            seen.add(city)
            unique_cities.append(city)

    logger.info(f"📍 Cidades únicas normalizadas: {unique_cities}")
    logger.debug(f"🔢 Total: {len(unique_cities)} cidades")

    return unique_cities


def map_city_indices(locations: List[str]) -> Dict[str, int]:
    """
    Gera um dicionário de mapeamento cidade_normalizada → índice na lista.

    :param locations: Lista de cidades únicas já normalizadas.
    :return: Dicionário {cidade: índice}
    """
    if not locations:
        raise ValueError(
            "❌ Lista de cidades (locations) está vazia. Não é possível mapear índices."
        )

    city_map = {city: idx for idx, city in enumerate(locations)}

    logger.info(f"🗺️ Mapeamento cidade → índice criado com {len(city_map)} entradas.")
    logger.debug(f"📌 city_map: {city_map}")

    return city_map


def build_city_index_and_matrix(
    df, trailers: List[Dict[str, Any]]
) -> Tuple[List[str], Dict[str, int], List[List[int]]]:
    """
    Constrói a lista de cidades, o mapeamento de índices e a matriz de distâncias.

    Retorna:
      - locations: lista ordenada de cidades normalizadas
      - city_index_map: dict cidade → índice
      - distance_matrix: matriz de distâncias inteiras (km)

    Lança exceções se os dados forem inválidos ou faltar coordenada.
    """
    # 1) Extrai cidades únicas
    locations = get_unique_cities(df, trailers)
    if not locations:
        raise ValueError("❌ Nenhuma cidade encontrada. Verifique os dados de entrada.")

    # 2) Mapeia cada cidade ao seu índice
    city_index_map = map_city_indices(locations)

    # 3) Constrói matriz de distâncias em float
    dist_f = _build_distance_matrix(locations)

    # 4) Converte distâncias para inteiros (km arredondados)
    distance_matrix: List[List[int]] = [[int(round(d)) for d in row] for row in dist_f]

    logger.info(f"🌍 Cidades únicas: {len(locations)}")
    logger.debug(
        f"📏 Tamanho da matriz de distâncias: {len(distance_matrix)}x{len(distance_matrix[0])}"
    )

    return locations, city_index_map, distance_matrix


def map_bases_to_indices(
    trailers: List[Dict[str, Any]],
    city_index_map: Dict[str, int],
) -> Tuple[List[int], List[int]]:
    """
    Mapeia a base de cada trailer para índices na matriz de localizações.

    Args:
      trailers: Lista de trailers com campo 'base_city'.
      city_index_map: Dicionário {cidade_normalizada: índice}.

    Returns:
      Tuple[List[int], List[int]]: Listas de índices de partida e de chegada para cada trailer.

    Raises:
      ValueError: Se não for possível mapear alguma base.
    """
    starts: List[int] = []
    ends: List[int] = []

    for trailer in trailers:
        trailer_id = trailer.get("id", "desconhecido")
        base_city = trailer.get("base_city")
        if not base_city:
            logger.warning(f"⚠️ Trailer {trailer_id} sem base_city definido; ignorado.")
            continue

        base_norm = norm(base_city)
        idx = city_index_map.get(base_norm)
        if idx is None:
            raise ValueError(
                f"🚫 Base '{base_norm}' do trailer ID {trailer_id} não está em city_index_map."
            )

        starts.append(idx)
        ends.append(idx)
        logger.info(f"🧭 Trailer ID {trailer_id}: base '{base_city}' → índice {idx}")

    if not starts:
        raise ValueError("❌ Nenhum trailer com base válida foi mapeado para índices.")

    return starts, ends
