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
    Para cada serviço com force_return=True obriga o nó de entrega (drop)
    a fechar a rota do veículo que o transportar:

        (VehicleVar(drop) == v)  ⇒  (NextVar(drop) == End(v))

    Optimização:
      • só cria booleans para veículos cujo End(v) está no domínio de NextVar
      • nada de Element() nem listas gigantes – < 30 k BoolVars no teu caso
    """
    solver = routing.solver()

    for i in range(n_srv):
        if not df["force_return"].iat[i]:
            continue

        drop = manager.NodeToIndex(i + n_srv)
        if drop < 0:
            continue  # nó já removido

        next_var = routing.NextVar(drop)
        vehicle_var = routing.VehicleVar(drop)

        # para cada veículo possível do 'drop'
        for v in range(routing.vehicles()):
            end_v = routing.End(v)

            # só se o sucessor 'end_v' faz parte do domínio de next_var
            if not next_var.Contains(end_v):
                continue

            b_vehicle = solver.IsEqualCstVar(vehicle_var, v)  # 0/1
            b_nextend = solver.IsEqualCstVar(next_var, end_v)  # 0/1

            # implicação: b_vehicle ⇒ b_nextend   (uso da desigualdade)
            solver.Add(b_vehicle <= b_nextend)

        logger.debug(
            "🔁 Forçando retorno: serviço %d (drop node %d) termina a rota",
            i,
            i + n_srv,
        )
