# backend\solver\callbacks\interno_penalty.py
from ortools.constraint_solver import pywrapcp


def interno_penalties(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df,
    low_penalty: int,
):
    """Penalidade baixa se viatura interna (CONS, PRC, '')."""
    for node in range(len(df)):
        pen = low_penalty if df.ins_short.iloc[node] in ("CONS", "PRC", "") else 1000
        routing.AddDisjunction([manager.NodeToIndex(node)], pen)
