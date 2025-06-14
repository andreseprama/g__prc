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
    distance_matrix: List[List[int]] | None,
    constraint_weights: dict[str, float],
    *,
    enable_interno: bool = False,
    enable_force_return: bool = False,
    enable_pickup_pairs: bool = True,
) -> None:
    """
    Orquestra todas as restrições necessárias:
    • Capacidade
    • (Opcional) pickup-delivery pairs
    """

    # 1) Dimensões de capacidade
    demand_cbs = create_demand_callbacks(df, manager, routing, depot_indices)
    add_dimensions_and_constraints(routing, trailers, demand_cbs)

    # 2) Pares pickup-delivery (se activado)
    if enable_pickup_pairs:
        for i in range(n_services):
            p = manager.NodeToIndex(1 + i)
            d = manager.NodeToIndex(1 + n_services + i)
            routing.AddPickupAndDelivery(p, d)
