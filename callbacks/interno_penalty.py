from ortools.constraint_solver import pywrapcp

# manter um atributo escondido no modelo para lembrar-nos
_TAG = "_disj_nodes"


def interno_penalties(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    pickup_ids: list[int],
    n_srv: int,
    weight: int = 1000,
) -> None:
    """
    Para cada serviço “interno” (pickup = delivery na mesma cidade)
    tornamos o par opcional com penalização «weight».

    Se algum dos 2 nós já estiver noutra disjunction, saltamos.
    """
    if not hasattr(routing, _TAG):
        setattr(routing, _TAG, set())
    used: set[int] = getattr(routing, _TAG)

    for srv_id in pickup_ids:
        p = manager.NodeToIndex(srv_id)
        d = manager.NodeToIndex(srv_id + n_srv)

        if p < 0 or d < 0:  # nó inexistente
            continue
        if p in used or d in used:  # já foi usado noutro AddDisjunction
            continue

        routing.AddDisjunction([p, d], weight)
        used.update([p, d])  # marca como usado
