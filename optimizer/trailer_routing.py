# backend\solver\optimizer\trailer_routing.py
from typing import Any, Callable
import pandas as pd
import logging

from backend.solver.utils import norm


def filter_services_by_category(
    df: pd.DataFrame, categorias_restritas: list[str], base_map: dict[str, str]
) -> pd.DataFrame:
    """
    Remove serviços cuja cidade de pickup não corresponde à base, apenas para as categorias filtradas.

    :param df: DataFrame com os serviços
    :param categorias_restritas: Lista de categorias (ex: ['P8', 'P9']) sujeitas a validação
    :param base_map: Dicionário cidade_normalizada -> base_normalizada
    """
    df = df.copy()

    def load_is_base(row) -> bool:
        load_city = str(row.get("load_city", "")).upper()
        expected_base = base_map.get(norm(load_city))
        return expected_base == norm(load_city)

    df["load_is_base"] = df.apply(load_is_base, axis=1)

    # Filtra apenas os serviços que NÃO violam a regra
    mask = ~(
        df["vehicle_category_name"]
        .str.upper()
        .isin([c.upper() for c in categorias_restritas])
        & (~df["load_is_base"])
    )

    result = df[mask].drop(columns=["load_is_base"])
    removed = len(df) - len(result)
    if removed > 0:
        logging.info(
            f"🚫 Removidos {removed} serviços por violarem regra P8/P9 pickup fora da base."
        )

    return result


def match_trailers_by_registry_trailer(
    trailers: list[dict[str, Any]], registry_trailer: str
) -> list[dict[str, Any]]:
    """
    Filtra lista de trailers ativos por matrícula.

    :param trailers: lista de trailers carregados
    :param registry_trailer: matrícula procurada
    """
    normalized = registry_trailer.strip().upper()
    result = [
        t for t in trailers if (t["registry_trailer"] or "").strip().upper() == normalized
    ]

    if not result:
        logging.warning(f"❌ Nenhum trailer encontrado com matrícula {registry_trailer}")
    return result


# 👮 Evita uso incorreto por nome antigo
__all__ = ["match_trailers_by_registry_trailer"]