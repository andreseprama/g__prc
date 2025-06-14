import pandas as pd
from typing import Optional
from backend.solver.utils import norm


def _get_base_for_city(city: str, base_map: dict[str, str]) -> Optional[str]:
    """
    Normaliza o nome da cidade e devolve a base correspondente (ou None).
    """
    if not city:
        return None
    city_norm = norm(city)
    return base_map.get(city_norm)


def must_return_to_base(row: pd.Series, base_map: dict[str, str]) -> bool:
    """
    True se a unload_city implicar retorno obrigatório a uma base.
    """
    return _get_base_for_city(str(row.get("unload_city", "")), base_map) is not None


def is_base_location(city: str, base_map: dict[str, str]) -> bool:
    """
    True se a cidade for exatamente uma base.
    """
    return _get_base_for_city(city, base_map) == norm(city or "")


def get_scheduled_base(row: pd.Series, base_map: dict[str, str]) -> Optional[str]:
    """
    Retorna a base agendada (load ou unload), escolhendo a load se existir,
    senão a unload, senão None.
    """
    load = _get_base_for_city(str(row.get("load_city", "")), base_map)
    if load:
        return load
    return _get_base_for_city(str(row.get("unload_city", "")), base_map)


def flag_return_and_base_fields(
    df: pd.DataFrame, base_map: dict[str, str]
) -> pd.DataFrame:
    """
    Adiciona colunas:
      - force_return: unload_city exige retorno a base?
      - load_is_base: load_city é base?
      - unload_is_base: unload_city é base?
    """
    df = df.copy()
    df["force_return"] = df.apply(lambda r: must_return_to_base(r, base_map), axis=1)
    df["load_is_base"] = (
        df["load_city"].astype(str).apply(lambda c: is_base_location(c, base_map))
    )
    df["unload_is_base"] = (
        df["unload_city"].astype(str).apply(lambda c: is_base_location(c, base_map))
    )
    return df
