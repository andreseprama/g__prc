# backend/solver/geocode.py

import os
import httpx
import unicodedata
import logging
from sqlalchemy import text
from urllib.parse import quote

from backend.solver.distance import register_coords, _norm

logger = logging.getLogger(__name__)

TOMTOM_KEY = os.getenv("TOMTOM_API_KEY")
TOMTOM_URL = "https://api.tomtom.com/search/2/geocode/{query}.json"


async def fetch_and_store_city(sess, city: str) -> None:
    """
    Se não houver coords conhecidas, busca no TomTom e persiste
    na tabela city_coords via SQL bruto, e registra no cache interno.
    """
    # 1) Normaliza e garante “, PORTUGAL”
    city_norm = _norm(city)
    query = city_norm
    if "PORTUGAL" not in city_norm:
        query = f"{city_norm}, PORTUGAL"

    # 2) Chama a API TomTom
    url = TOMTOM_URL.format(query=quote(query))
    resp = httpx.get(url, params={"key": TOMTOM_KEY, "limit": 1})
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])
    if not results:
        raise RuntimeError(f"TomTom não retornou coords para '{query}'")

    lat = results[0]["position"]["lat"]
    lon = results[0]["position"]["lon"]
    logger.info(f"🌐 Geocoded {city_norm} → ({lat}, {lon})")

    # 3) Persiste na tabela city_coords usando SQL bruto
    await sess.execute(
        text(
            """
            INSERT INTO public.city_coords(city_norm, latitude, longitude)
            VALUES (:city, :lat, :lon)
            ON CONFLICT (city_norm) DO NOTHING
            """
        ),
        {"city": city_norm, "lat": lat, "lon": lon},
    )
    await sess.commit()

    # 4) Atualiza cache interno de distance.py
    register_coords({city_norm: (lat, lon)})
