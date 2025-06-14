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
    distance_matrix,  # não usado nesta versão “só capacidade”
    constraint_weights: dict[str, float],
    *,
    enable_pickup_pairs: bool = True,
) -> None:
    """
    1) Cria dimensões de capacidade (CEU, LIG, FUR, ROD)
    2) (Opcional) Pares pickup-delivery
    """
    # ——— 1) capacidade ———
    demand_cbs = create_demand_callbacks(df, manager, routing, depot_indices)
    add_dimensions_and_constraints(routing, trailers, demand_cbs)

    # ——— 2) pickup-delivery ———
    if enable_pickup_pairs:
        # vamos usar a dimensão “CEU” para impor precedência pickup→delivery
        ceu_dim = routing.GetDimensionOrDie("CEU")
        for i in range(n_services):
            # os nós no modelo correspondem a:
            #   depósito = 0
            #   pickup_i = 1 + i
            #   delivery_i = 1 + n_services + i
            p_idx = manager.NodeToIndex(1 + i)
            d_idx = manager.NodeToIndex(1 + n_services + i)

            routing.AddPickupAndDelivery(p_idx, d_idx)
            # força mesmo veículo
            routing.solver().Add(routing.VehicleVar(p_idx) == routing.VehicleVar(d_idx))
            # força pickup antes de delivery (pela dimensão CEU)
            routing.solver().Add(ceu_dim.CumulVar(p_idx) <= ceu_dim.CumulVar(d_idx))
