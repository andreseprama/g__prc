# backend\solver\optimizer\utils_df.py
import pandas as pd
from backend.solver.utils import norm


def normalize_city_fields(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["load_city"] = df["load_city"].apply(lambda x: norm(x) if pd.notnull(x) else "")
    df["unload_city"] = df["unload_city"].apply(lambda x: norm(x) if pd.notnull(x) else "")
    return df


def calculate_ceu(df: pd.DataFrame) -> pd.DataFrame:
    def ceu(row: pd.Series) -> float:
        name = (row.get("vehicle_category_name") or "").lower()
        if "furg" in name or "rodado" in name:
            return 1.5
        if "moto" in name:
            return 0.3
        return 1.0

    df["ceu"] = df.apply(ceu, axis=1)
    df["ceu_int"] = (
        df["ceu"].fillna(0).astype(float).apply(lambda x: int(round(x * 10)))
    )
    return df


def add_base_flags(df: pd.DataFrame, base_map: dict) -> pd.DataFrame:
    def is_base(row, col):
        city = str(row.get(col, "")).upper()
        expected = base_map.get(norm(city))
        return expected == norm(city)

    df["load_is_base"] = df.apply(lambda r: is_base(r, "load_city"), axis=1)
    df["unload_is_base"] = df.apply(lambda r: is_base(r, "unload_city"), axis=1)
    df["force_return"] = df["unload_city"].map(lambda c: norm(c) in base_map)

    return df


def make_service_reg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera coluna service_reg única por linha com base em colunas id + matricula.
    """
    if "service_reg" not in df.columns:
        if {"id", "matricula"}.issubset(df.columns):
            df["service_reg"] = df["id"].astype(str) + "_" + df["matricula"].astype(str)
        elif "id" in df.columns:
            df["service_reg"] = df["id"].astype(str)
        else:
            raise ValueError("❌ Não foi possível construir service_reg: faltam colunas id e/ou matricula.")
    return df