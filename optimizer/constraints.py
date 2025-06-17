#/app/backend/solver/optimizer/constraints.py
from typing import List
import pandas as pd
from ortools.constraint_solver import pywrapcp

from backend.solver.routing import (
    create_demand_callbacks,
    add_dimensions_and_constraints,
)

from backend.solver.utils import norm


def apply_all_constraints(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df: pd.DataFrame,
    trailers: List[dict],
    n_services: int,
    depot_indices: List[int],
    distance_matrix: List[List[int]],  # ainda não usado
    constraint_weights: dict[str, float],
    *,
    enable_pickup_pairs: bool = True,
) -> None:
    from backend.solver.optimizer.city_mapping import map_city_indices

    # Capacidade (CEU)
    cb_indices, _ = create_demand_callbacks(df, manager, routing, depot_indices)
    add_dimensions_and_constraints(routing, trailers, cb_indices)

    if not enable_pickup_pairs:
        return

    ceu_dim = routing.GetDimensionOrDie("CEU")

    # Mapeamento cidades → índices
    all_cities = df["load_city"].tolist() + df["unload_city"].tolist()
    all_cities = list({c for c in all_cities if isinstance(c, str)})
    city_index_map = map_city_indices([c.upper().strip() for c in all_cities])

    for i, row in df.iterrows():
        load = row["load_city"]
        unload = row["unload_city"]

        try:
            p_node = city_index_map[norm(load)]
            d_node = city_index_map[norm(unload)]

            p_idx = manager.NodeToIndex(p_node)
            d_idx = manager.NodeToIndex(d_node)

            routing.AddPickupAndDelivery(p_idx, d_idx)
            routing.solver().Add(routing.VehicleVar(p_idx) == routing.VehicleVar(d_idx))
            routing.solver().Add(ceu_dim.CumulVar(p_idx) <= ceu_dim.CumulVar(d_idx))
        except Exception as e:
            print(f"❌ Erro aplicando pickup-delivery para linha {i}: {e}")