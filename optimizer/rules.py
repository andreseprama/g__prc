# backend\solver\optimizer\rules.py
import pandas as pd
from typing import Callable
from backend.solver.utils import norm


def must_return_to_base(row: pd.Series, base_map: dict[str, str]) -> bool:
    """
    Retorna True se a cidade de descarregamento implicar retorno obrigatório a uma base.
    """
    unload_city_norm = norm(str(row.get("unload_city", "")))
    return base_map.get(unload_city_norm) is not None


def is_base_location(city: str, base_map: dict[str, str]) -> bool:
    """
    Verifica se uma cidade corresponde a uma base, segundo o mapeamento normalizado.
    """
    if not isinstance(city, str):
        return False
    city_norm = norm(city)
    expected_base = base_map.get(city_norm)
    return expected_base == city_norm


def flag_return_and_base_fields(
    df: pd.DataFrame, base_map: dict[str, str]
) -> pd.DataFrame:
    """
    Adiciona colunas:
      - `force_return`: se a descarga está numa cidade com base
      - `load_is_base`: se o pickup está numa cidade de base
      - `unload_is_base`: se o drop-off está numa cidade de base
    """
    df = df.copy()

    df["force_return"] = df.apply(
        lambda row: must_return_to_base(row, base_map), axis=1
    )

    df["load_is_base"] = df["load_city"].apply(lambda c: is_base_location(c, base_map))
    df["unload_is_base"] = df["unload_city"].apply(
        lambda c: is_base_location(c, base_map)
    )

    return df
