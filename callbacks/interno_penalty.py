# backend/solver/callbacks/interno_penalty.py
from ortools.constraint_solver import pywrapcp


def interno_penalties(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    pickup_ids: list[int],
    n_srv: int,
    weight: int = 1000,
) -> None:
    """
    Penaliza serviços internos (pickup & delivery na mesma cidade)
    permitindo descartar o par todo, MAS só se esses nós ainda não
    tiverem sido usados noutra disjunção.
    """
    if not hasattr(routing, "_disj_nodes"):
        routing._disj_nodes = set()

    for srv_id in pickup_ids:
        p = manager.NodeToIndex(srv_id)
        d = manager.NodeToIndex(srv_id + n_srv)

        if p < 0 or d < 0:  # nó inexistente
            continue

        # Se qualquer dos nós já for opcional, salta — outra regra já tratou
        if (
            routing.AssignmentOrNull(p) is not None
            or routing.AssignmentOrNull(d) is not None
        ):
            continue

        routing.AddDisjunction([p, d], weight)
