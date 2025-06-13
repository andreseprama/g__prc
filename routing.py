from ortools.constraint_solver import pywrapcp
from typing import List, Callable, Dict
import pandas as pd
import logging

DEFAULT_PENALTY = 99999  # ou outro valor alto aceitável


def safe_dist_lookup(dist_matrix, manager, i, j):
    try:
        if not isinstance(i, int) or not isinstance(j, int):
            logging.warning(f"⚠️ Índices não inteiros: i={i}, j={j}")
            return DEFAULT_PENALTY

        if i < 0 or j < 0:
            logging.warning(f"⚠️ Índices negativos: i={i}, j={j}")
            return DEFAULT_PENALTY

        from_node = manager.IndexToNode(i)
        to_node = manager.IndexToNode(j)

        if from_node >= len(dist_matrix) or to_node >= len(dist_matrix):
            logging.warning(
                f"⚠️ Índices fora dos limites da matriz: from_node={from_node}, to_node={to_node}"
            )
            return DEFAULT_PENALTY

        return dist_matrix[from_node][to_node]

    except Exception as e:
        logging.error(f"⛔ Erro em dist lookup: i={i}, j={j}, erro={e}")
        return DEFAULT_PENALTY



def build_routing_model(
    n_nodes: int,
    n_vehicles: int,
    starts: List[int],
    ends: List[int],
    dist_matrix: List[List[int]],
):
    manager = pywrapcp.RoutingIndexManager(n_nodes, n_vehicles, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    def distance_callback(i: int, j: int) -> int:
        return safe_dist_lookup(dist_matrix, manager, i, j)

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    return manager, routing


def create_demand_callbacks(
    df: pd.DataFrame,
    manager: pywrapcp.RoutingIndexManager,
    routing: pywrapcp.RoutingModel,
    depot_indices: List[int],
) -> Dict[str, int]:
    def mk_cb(kind: str) -> int:
        def demand(index: int) -> int:
            if index >= manager.Size():
                raise ValueError(f"Índice inválido: {index} fora do range permitido.")
            node = manager.IndexToNode(index)
            if node in depot_indices:
                return 0
            pickup = node < len(df)
            base = node if pickup else node - len(df)
            cat = (df.vehicle_category_name.iat[base] or "").lower()

            if kind == "ceu":
                if "ceu_int" not in df.columns:
                    raise KeyError("Coluna 'ceu_int' ausente no DataFrame")
                val = int(df.ceu_int.iat[base])
                return val if pickup else 0
            if kind == "lig":
                return 0 if not pickup else (0 if "moto" in cat else 1)
            if kind == "fur":
                return 0 if not pickup else (1 if "furg" in cat else 0)
            if kind == "rod":
                return 0 if not pickup else (1 if "rodado" in cat else 0)
            return 0

        return routing.RegisterUnaryTransitCallback(demand)

    return {
        "ceu": mk_cb("ceu"),
        "lig": mk_cb("lig"),
        "fur": mk_cb("fur"),
        "rod": mk_cb("rod"),
    }


def add_dimensions_and_constraints(
    routing: pywrapcp.RoutingModel, trailers: list, callbacks: dict[str, int]
):
    ceu_cap = [int(round(float(t["ceu_max"]) * 10)) for t in trailers]
    lig_cap = [int(t["ligeiro_max"] or 0) for t in trailers]
    fur_cap = [int(t["furgo_max"] or 0) for t in trailers]
    rod_cap = [int(t["rodado_max"] or 0) for t in trailers]

    for name, caps, cb_name in [
        ("CEU", ceu_cap, "ceu"),
        ("LIG", lig_cap, "lig"),
        ("FUR", fur_cap, "fur"),
        ("ROD", rod_cap, "rod"),
    ]:
        if any(c > 0 for c in caps):
            if cb_name not in callbacks:
                raise ValueError(f"Callback '{cb_name}' não encontrado.")
            print(f"➡️ Dimensão: {name}")
            print(f"  Capacidades: {caps}")
            print(f"  Callback disponível? {'sim' if cb_name in callbacks else 'não'}")
            print(f"  Callback {cb_name}: {callbacks.get(cb_name)}")

            routing.AddDimensionWithVehicleCapacity(
                callbacks[cb_name], 0, caps, True, name
            )


def add_distance_penalty(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    trailers: list,
    dist_matrix: List[List[int]],
    max_km: int,
    penalty_per_km: int,
):
    dim_name = "DIST"

    def dist_cb(i: int, j: int) -> int:
        return safe_dist_lookup(dist_matrix, manager, i, j)

    cb_index = routing.RegisterTransitCallback(dist_cb)
    routing.AddDimension(
        cb_index,
        0,  # no slack
        DEFAULT_PENALTY,
        True,
        dim_name,
    )

    dim = routing.GetDimensionOrDie(dim_name)
    dim.SetGlobalSpanCostCoefficient(penalty_per_km)

    for v in range(routing.vehicles()):
        dim.CumulVar(routing.End(v)).SetMax(max_km)
