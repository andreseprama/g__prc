#backend/solver/optimizer/rules.py
import pandas as pd
from typing import Optional
from backend.solver.utils import norm
import logging



logger = logging.getLogger(__name__)

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
    if not city:
        logger.debug("🔎 is_base_location: cidade vazia")
        return False

    city_norm = norm(city)
    base_cities = set(base_map.keys())  # ← city_norms
    return city_norm in base_cities


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

    # Log se alguma cidade estiver vazia
    empty_cities = df[df["load_city"].str.strip() == ""]
    if not empty_cities.empty:
        logging.warning("🚨 Entradas com load_city vazia detectadas:")
        print("\U0001f6a8 Entradas com cidade vazia detectadas:")
        print(empty_cities[["matricula", "load_city", "unload_city"]])

    # Aplicar marcações de base
    df["force_return"] = df.apply(lambda r: must_return_to_base(r, base_map), axis=1)
    df["load_is_base"] = df["load_city"].astype(str).apply(
        lambda c: is_base_location(c, base_map)
    )
    df["unload_is_base"] = df["unload_city"].astype(str).apply(
        lambda c: is_base_location(c, base_map)
    )

    print("\U0001f50e Bases detectadas:")
    print(df[["matricula", "load_city", "unload_city", "load_is_base", "unload_is_base"]])

    return df
