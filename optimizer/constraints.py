"""
Aplicação das várias restrições e penalizações ao modelo de routing
(OR-Tools) usado pelo optimizador.

• Capacidade (CEU, LIG, FUR, ROD)  →  add_dimensions_and_constraints
• Penalização de distância / limite por viatura  →  add_distance_penalty
• (Opcional) Penalização para serviços internos  →  interno_penalties
• (Opcional) Forçar retorno à base                →  add_force_return_constraints
• (Opcional) Pares pickup-delivery                →  add_pickup_delivery_pairs
"""

from __future__ import annotations

from typing import List
import pandas as pd
from ortools.constraint_solver import pywrapcp

from backend.solver.routing import (
    create_demand_callbacks,
    add_dimensions_and_constraints,
    add_distance_penalty,
)

# Descomenta se/quando voltares a usar estas regras extra
# from backend.solver.callbacks.interno_penalty import interno_penalties
# from backend.solver.location_rules import add_force_return_constraints

from backend.solver.utils import norm


# ──────────────────────────────────────────────────────────────────────────────
#  OPTIONAL  —— precedência pickup / delivery
# ──────────────────────────────────────────────────────────────────────────────
def add_pickup_delivery_pairs(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df: pd.DataFrame,
) -> None:
    """
    Para cada serviço i:
        pickup    = i
        delivery  = i + n_srv
    • Obriga a ficar no MESMO veículo
    • Obriga pickup acontecer antes do delivery
    """
    n_srv = len(df)
    # Usamos a dimensão “DIST” criada em add_distance_penalty
    dist_dim = routing.GetDimensionOrDie("DIST")

    for i in range(n_srv):
        p = manager.NodeToIndex(i)
        d = manager.NodeToIndex(i + n_srv)

        # protecção extra caso o nó não exista
        if p < 0 or d < 0:
            continue

        routing.AddPickupAndDelivery(p, d)
        routing.solver().Add(routing.VehicleVar(p) == routing.VehicleVar(d))
        routing.solver().Add(dist_dim.CumulVar(p) <= dist_dim.CumulVar(d))


# ──────────────────────────────────────────────────────────────────────────────
#  FUNÇÃO PRINCIPAL  —— aplica TODAS as constraints
# ──────────────────────────────────────────────────────────────────────────────
def apply_all_constraints(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df: pd.DataFrame,
    trailers: List[dict],
    n_services: int,
    depot_indices: List[int],
    distance_matrix: List[List[int]],
    constraint_weights: dict[str, float],
    *,
    enable_interno: bool = False,
    enable_force_return: bool = False,
    enable_pickup_pairs: bool = False,
) -> None:
    """
    Orquestra todas as restrições necessárias para o problema de VRP.

    Parameters
    ----------
    routing / manager : OR-Tools core objects
    df                : DataFrame com serviços (|df| = n_services)
    trailers          : lista de trailers activos (dicts)
    n_services        : len(df)
    depot_indices     : índices (no manager) das bases
    distance_matrix   : matriz de distâncias enteras (km)
    constraint_weights: pesos configuráveis vindos da BD
    enable_*          : activa/desactiva regras opcionais
    """

    # ── 1. Capacidade ────────────────────────────────────────────────────────
    demand_cbs = create_demand_callbacks(df, manager, routing, depot_indices)
    add_dimensions_and_constraints(routing, trailers, demand_cbs)

    # ── 2. Penalização de distância + limite por viatura ─────────────────────
    add_distance_penalty(
        routing,
        manager,
        distance_matrix,
        penalty_per_km=int(constraint_weights.get("PENALIDADE_DIST_KM", 3)),
        max_km=int(constraint_weights.get("MAX_DIST_POR_TRAILER", 400)),
    )

    # ── 3. (Opcional) Penalizar serviços “internos” ──────────────────────────
    if enable_interno:
        from backend.solver.callbacks.interno_penalty import (
            interno_penalties,
        )  # local import

        low_prio_ids: list[int] = [
            i
            for i in range(n_services)
            if norm(df.load_city_description.iat[i])
            == norm(df.unload_city_description.iat[i])
        ]
        interno_penalties(
            routing=routing,
            manager=manager,
            pickup_ids=low_prio_ids,
            n_srv=n_services,
            weight=int(constraint_weights.get("INTERNO_LOW_PEN", 1000)),
        )

    # ── 4. (Opcional) Forçar retorno à base ──────────────────────────────────
    if enable_force_return:
        from backend.solver.location_rules import (
            add_force_return_constraints,
        )  # local import

        add_force_return_constraints(
            routing=routing,
            manager=manager,
            df=df,
            n_srv=n_services,
        )

    # ── 5. (Opcional) Pares pickup-delivery ──────────────────────────────────
    if enable_pickup_pairs:  # ← mantém False
        add_pickup_delivery_pairs(routing, manager, df)
