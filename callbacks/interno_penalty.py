# backend\solver\callbacks\interno_penalty.py
from ortools.constraint_solver import pywrapcp


def interno_penalties(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    pickup_ids: list[int],  # ids de serviço (0..n_srv-1) que podem ser descartados
    n_srv: int,
    weight: int = 1000,
) -> None:
    """
    Aplica penalidade a serviços *internos* de baixa prioridade,
    permitindo que todo o par (pickup + delivery) seja removido.
    """

    for srv_id in pickup_ids:
        p = manager.NodeToIndex(srv_id)  # pickup
        d = manager.NodeToIndex(srv_id + n_srv)  # delivery

        # OR-Tools devolve -1 se o nó já não existe → salta
        if p < 0 or d < 0:
            continue

        # Penalidade sobre o par inteiro
        routing.AddDisjunction([p, d], weight)
