from __future__ import annotations
from typing import List
import pandas as pd
from ortools.constraint_solver import pywrapcp

from backend.solver.routing import (
    create_demand_callbacks,
    add_dimensions_and_constraints,
)


# ────────────────────────────────────────────
# helper: cria pares pickup-delivery
# ────────────────────────────────────────────
def add_pickup_delivery_pairs(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df: pd.DataFrame,
) -> None:
    """Para cada serviço i cria (pickup=i, delivery=i+n_srv)."""
    n_srv = len(df)

    for i in range(n_srv):
        p = manager.NodeToIndex(i)
        d = manager.NodeToIndex(i + n_srv)

        if p < 0 or d < 0:  # nó inexistente → salta
            continue

        routing.AddPickupAndDelivery(p, d)
        # nada mais é preciso: OR-Tools já impõe:
        #   • mesmo veículo
        #   • pickup antecede delivery


# ────────────────────────────────────────────
# função principal
# ────────────────────────────────────────────
def apply_all_constraints(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df: pd.DataFrame,
    trailers: List[dict],
    n_services: int,
    depot_indices: List[int],
    distance_matrix,  # ignorado nesta versão “só capacidade”
    constraint_weights: dict[str, float],
    *,
    enable_pickup_pairs: bool = True,
) -> None:
    # 1) capacidade (CEU / LIG / FUR / ROD)
    demand_cbs = create_demand_callbacks(df, manager, routing, depot_indices)
    add_dimensions_and_constraints(routing, trailers, demand_cbs)

    # 2) pickup-delivery
    if enable_pickup_pairs:
        add_pickup_delivery_pairs(routing, manager, df)
