"""
Regras para mapear cidades-base e forçar retorno dos serviços.
Compatível com OR-Tools 9.7 (não usa SetAllowedTransitEdgesForNode).
"""

from typing import Dict
import logging
import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from ortools.constraint_solver import pywrapcp
from backend.solver.utils import norm

logger = logging.getLogger(__name__)


# ───────────────────────── helpers BD ──────────────────────────────────────────
async def fetch_city_base_map(sess: AsyncSession) -> Dict[str, str]:
    q = await sess.execute(
        text(
            """
            SELECT city_norm, base_norm
              FROM rule_return_city
             WHERE base_norm IS NOT NULL
            """
        )
    )
    return {r.city_norm: r.base_norm for r in q}


# ───────────────────── rewrites P8 / P9 ────────────────────────────────────────
async def rewrite_load_city_if_return(df: pd.DataFrame, sess: AsyncSession) -> None:
    city_to_base = await fetch_city_base_map(sess)

    def override(row: pd.Series) -> str:
        trailer = str(row.get("vehicle_category_name", "")).upper()
        cur = str(row.get("load_city", ""))
        return (
            city_to_base.get(norm(cur), cur)
            if trailer.startswith(("P8", "P9"))
            else cur
        )

    df["orig_load_city"] = df["load_city"]
    df["load_city"] = df.apply(override, axis=1)


async def rewrite_unload_city_if_return(df: pd.DataFrame, sess: AsyncSession) -> None:
    city_to_base = await fetch_city_base_map(sess)

    def override(row: pd.Series) -> str:
        trailer = str(row.get("vehicle_category_name", "")).upper()
        cur = str(row.get("unload_city", ""))
        return (
            city_to_base.get(norm(cur), cur)
            if trailer.startswith(("P8", "P9"))
            else cur
        )

    df["orig_unload_city"] = df["unload_city"]
    df["unload_city"] = df.apply(override, axis=1)


# ─────────────────── força retorno com Element() ──────────────────────────────
def add_force_return_constraints(routing, manager, df, n_srv):
    solver = routing.solver()

    for i in range(n_srv):
        if not df["force_return"].iat[i]:
            continue

        drop = manager.NodeToIndex(i + n_srv)
        if drop < 0:
            continue  # nó removido

        next_var = routing.NextVar(drop)
        vehicle_var = routing.VehicleVar(drop)

        # usa apenas intervalos Min/Max  -->  nunca chama Contains()
        lo, hi = next_var.Min(), next_var.Max()

        for v in range(routing.vehicles()):
            end_v = routing.End(v)
            if end_v < lo or end_v > hi:
                continue  # fora do domínio

            b_v = solver.IsEqualCstVar(vehicle_var, v)
            b_end = solver.IsEqualCstVar(next_var, end_v)
            solver.Add(b_v <= b_end)  # (b_v ⇒ b_end)
