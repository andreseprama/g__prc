# backend\solver\callbacks\ceu_cap.py
from ortools.constraint_solver import pywrapcp


def ceu_dimension(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df,
    ceu_cap: float,
):
    """Capacity constraint em CEU."""

    def demand(idx):
        node = manager.IndexToNode(idx)
        return df.ceu_std.iloc[node] if node < len(df) else 0

    demand_cb = routing.RegisterUnaryTransitCallback(demand)
    routing.AddDimensionWithVehicleCapacity(
        demand_cb, 0, [ceu_cap] * routing.vehicles(), True, "Capacity"
    )
