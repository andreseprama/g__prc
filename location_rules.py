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
def add_force_return_constraints(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df: pd.DataFrame,
    n_srv: int,
) -> None:
    """
    Para cada serviço com force_return=True obriga o nó de entrega
    a ser seguido imediatamente do End do veículo que o servir.

        NextVar(drop) == Element(ends[], VehicleVar(drop))

    * Compatível com OR-Tools <= 9.7
    * Cria 1 IntExpr por serviço (leve em memória)
    """
    solver = routing.solver()
    ends = [routing.End(v) for v in range(routing.vehicles())]

    for i in range(n_srv):
        if not df["force_return"].iat[i]:
            continue

        drop = manager.NodeToIndex(i + n_srv)
        if drop < 0:  # pode ter sido removido por disjunção
            continue

        next_var = routing.NextVar(drop)
        vehicle_var = routing.VehicleVar(drop)
        end_expr = solver.Element(ends, vehicle_var)  # End(vehicle)

        solver.Add(next_var == end_expr)

        logger.debug(
            "🔁 Forçando retorno: serviço %d (drop node %d) termina a rota",
            i,
            i + n_srv,
        )
