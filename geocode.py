# backend/solver/geocode.py

import os
import httpx
import logging
from sqlalchemy import text
from urllib.parse import quote

from backend.solver.distance import register_coords, _norm

logger = logging.getLogger(__name__)

TOMTOM_KEY = os.getenv("TOMTOM_API_KEY")
TOMTOM_URL = "https://api.tomtom.com/search/2/geocode/{query}.json"


async def fetch_and_store_city(sess, city: str) -> None:
    """
    Consulta localmente a tabela city_coords.
    Se não houver coordenadas, busca na API TomTom e persiste.
    Sempre registra no cache interno da distância.
    """
    city_norm = _norm(city)

    # 1) Tenta buscar coordenadas locais já persistidas
    result = await sess.execute(
        text("SELECT latitude, longitude FROM public.city_coords WHERE city_norm = :city"),
        {"city": city_norm},
    )
    row = result.first()

    if row:
        lat, lon = row
        logger.debug(f"📍 Coordenadas existentes para {city_norm}: ({lat}, {lon})")
    else:
        # 2) Monta query e faz geocodificação
        query = f"{city_norm}, PORTUGAL" if "PORTUGAL" not in city_norm else city_norm
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

        # 3) Persiste na base
        await sess.execute(
            text("""
                INSERT INTO public.city_coords(city_norm, latitude, longitude)
                VALUES (:city, :lat, :lon)
                ON CONFLICT (city_norm) DO NOTHING
            """),
            {"city": city_norm, "lat": lat, "lon": lon},
        )
        await sess.commit()

    # 4) Registra no cache interno
    register_coords({city_norm: (lat, lon)})
