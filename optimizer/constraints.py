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


def apply_all_constraints(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df: pd.DataFrame,
    trailers: List,
    n_services: int,
    depot_indices: List[int],
    distance_matrix: List[List[int]],
    constraint_weights: dict[str, float],
):
    """
    Aplica todas as restrições e penalizações ao modelo de roteamento.

    :param routing: Modelo de roteamento OR-Tools
    :param manager: Gerenciador de índices OR-Tools
    :param df: DataFrame com os serviços
    :param trailers: Lista de trailers ativos
    :param n_services: Quantidade de serviços (pickup/delivery)
    :param depot_indices: Índices de início/fim de cada trailer
    :param distance_matrix: Matriz de distâncias entre cidades
    :param constraint_weights: Dicionário com os pesos das penalizações
    """

    # 🧮 Callback de demanda (CEU, FURG, RODADO, etc.)
    demand_callbacks = create_demand_callbacks(df, manager, routing, depot_indices)
    add_dimensions_and_constraints(routing, trailers, demand_callbacks)

    # 🚚 Penalização para serviços internos (mesma cidade)
    interno_penalties(
        routing,
        manager,
        df,
        low_penalty=int(constraint_weights.get("INTERNO_LOW_PEN", 10)),
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

    # 🔁 Força retorno à base se necessário (base definida para a cidade de entrega)
    add_force_return_constraints(routing, manager, df, n_srv=n_services)
