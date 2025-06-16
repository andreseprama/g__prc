#/app/backend/solver/optimizer/constraints.py
from typing import List
import pandas as pd
from ortools.constraint_solver import pywrapcp

from backend.solver.routing import (
    create_demand_callbacks,
    add_dimensions_and_constraints,
)


def apply_all_constraints(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df: pd.DataFrame,
    trailers: List[dict],
    n_services: int,
    depot_indices: List[int],
    distance_matrix,  # não usado, reservado para futuro
    constraint_weights: dict[str, float],
    *,
    enable_pickup_pairs: bool = True,
) -> None:
    """
    Aplica todas as constraints de capacidade e precedência:

    1) CEU, LIG, FUR, ROD como dimensões de capacidade
    2) Pickup → Delivery: mesma viatura, pickup precede delivery
    """
    # ——— 1) Capacidade ———
    cb_indices, _ = create_demand_callbacks(df, manager, routing, depot_indices)
    add_dimensions_and_constraints(routing, trailers, cb_indices)

    # ——— 2) Pickup-delivery ———
    if enable_pickup_pairs:
        ceu_dim = routing.GetDimensionOrDie("CEU")
        for i in range(n_services):
            p_idx = manager.NodeToIndex(1 + i)
            d_idx = manager.NodeToIndex(1 + n_services + i)

            routing.AddPickupAndDelivery(p_idx, d_idx)
            routing.solver().Add(routing.VehicleVar(p_idx) == routing.VehicleVar(d_idx))
            routing.solver().Add(ceu_dim.CumulVar(p_idx) <= ceu_dim.CumulVar(d_idx))
