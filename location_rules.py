# backend/solver/location_rules.py

from typing import Dict, Any, List
import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from backend.solver.utils import norm
from ortools.constraint_solver import pywrapcp
import logging


async def fetch_city_base_map(sess: AsyncSession) -> Dict[str, str]:
    """
    Retorna dicionário {cidade_normalizada: base_normalizada}
    baseado em rule_return_city.
    """
    q = await sess.execute(
        text(
            """
            SELECT city_norm, base_norm
            FROM rule_return_city
            WHERE base_norm IS NOT NULL
            """
        )
    )
    return {row.city_norm: row.base_norm for row in q}


async def rewrite_load_city_if_return(df: pd.DataFrame, sess: AsyncSession) -> None:
    """
    Para linhas com load_city ∈ cidades retornáveis e trailer ∈ P8/P9,
    reescreve a cidade de carga para base associada.
    Também preserva coluna original em orig_load_city.
    """
    city_to_base = await fetch_city_base_map(sess)

    def override_city(row: pd.Series) -> str:
        trailer = str(row.get("vehicle_category_name", "")).upper()
        current_city = str(row.get("load_city", ""))
        if trailer.startswith("P8") or trailer.startswith("P9"):
            return city_to_base.get(norm(current_city), current_city)
        return current_city

    df["orig_load_city"] = df["load_city"]
    df["load_city"] = df.apply(override_city, axis=1)


async def rewrite_unload_city_if_return(df: pd.DataFrame, sess: AsyncSession) -> None:
    """
    Para linhas com unload_city ∈ cidades retornáveis e trailer ∈ P8/P9,
    reescreve a cidade de descarga para base associada.
    Também preserva coluna original em orig_unload_city.
    """
    city_to_base = await fetch_city_base_map(sess)

    def override_city(row: pd.Series) -> str:
        trailer = str(row.get("vehicle_category_name", "")).upper()
        current_city = str(row.get("unload_city", ""))
        if trailer.startswith("P8") or trailer.startswith("P9"):
            return city_to_base.get(norm(current_city), current_city)
        return current_city

    df["orig_unload_city"] = df["unload_city"]
    df["unload_city"] = df.apply(override_city, axis=1)


def add_force_return_constraints(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df: pd.DataFrame,
    n_srv: int,
):
    solver = routing.solver()

    for i in range(n_srv):
        if not df["force_return"].iat[i]:
            continue

        drop = manager.NodeToIndex(i + n_srv)
        if drop < 0:  # nó removido por disjunção
            continue

        next_var = routing.NextVar(drop)
        if next_var is None:  # OR-Tools não criou var p/ este nó
            continue

        for v in range(routing.vehicles()):
            end_v = routing.End(v)
            if end_v < 0:  # veículo inexistente (defensivo)
                continue
            if not next_var.Contains(end_v):
                continue  # end_v fora do domínio

            b_vehicle = solver.IsEqualCstVar(routing.VehicleVar(drop), v)
            b_nextend = solver.IsEqualCstVar(next_var, end_v)

            # se b_vehicle == 1  ⇒  b_nextend == 1
            solver.Add(b_vehicle <= b_nextend)

        logging.debug(
            "🔁 Forçando retorno: serviço %d (drop node %d) termina a rota",
            i,
            i + n_srv,
        )
