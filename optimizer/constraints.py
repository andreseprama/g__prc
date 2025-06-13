# backend/solver/optimizer/constraints.py
from ortools.constraint_solver import pywrapcp
import pandas as pd
from typing import List

from backend.solver.routing import (
    create_demand_callbacks,
    add_dimensions_and_constraints,
    add_distance_penalty,
)
from backend.solver.callbacks.interno_penalty import interno_penalties
from backend.solver.location_rules import add_force_return_constraints
from backend.solver.utils import norm


def apply_all_constraints(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df: pd.DataFrame,
    trailers: List,
    n_services: int,
    depot_indices: List[int],
    distance_matrix: List[List[int]],
    constraint_weights: dict[str, float],
) -> None:
    """
    Aplica todas as restrições e penalizações ao modelo de roteamento.
    """

    # 🧮 Dimensões de capacidade e demais constraints
    demand_callbacks = create_demand_callbacks(df, manager, routing, depot_indices)
    add_dimensions_and_constraints(routing, trailers, demand_callbacks)

    # 🚚 Penalização para serviços "internos" (mesma cidade)
    low_prio_ids: list[int] = [
        i
        for i in range(len(df))
        if norm(df.load_city_description.iat[i])
        == norm(df.unload_city_description.iat[i])
    ]
    interno_penalties(
        routing=routing,
        manager=manager,
        pickup_ids=low_prio_ids,
        n_srv=len(df),
        weight=int(constraint_weights.get("INTERNO_LOW_PEN", 1000)),
    )

    # 📏 Penalização de distância + limite máximo por trailer
    add_distance_penalty(
        routing=routing,
        manager=manager,
        trailers=trailers,
        penalty_per_km=int(constraint_weights.get("PENALIDADE_DIST_KM", 3)),
        max_km=int(constraint_weights.get("MAX_DIST_POR_TRAILER", 400)),
        dist_matrix=distance_matrix,
    )

    # 🔁 Força retorno à base (para serviços marcados)
    add_force_return_constraints(
        routing=routing,
        manager=manager,
        df=df,
        n_srv=n_services,
    )


def add_pickup_delivery_pairs(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df: pd.DataFrame,
) -> None:
    """
    Para cada serviço i, cria dois nós:
      • pickup  = i
      • delivery = i + n_srv
    e força:
      • mesmo veículo
      • pickup precede delivery
    """
    n_srv = len(df)

    for i in range(n_srv):
        p = manager.NodeToIndex(i)
        d = manager.NodeToIndex(i + n_srv)

        routing.AddPickupAndDelivery(p, d)

        # Mesma viatura
        routing.solver().Add(routing.VehicleVar(p) == routing.VehicleVar(d))
        # Pickup antes do delivery
        routing.solver().Add(
            routing.CumulVar(p, "Distance") <= routing.CumulVar(d, "Distance")
        )
