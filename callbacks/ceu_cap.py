# backend\solver\callbacks\ceu_cap.py
from ortools.constraint_solver import pywrapcp


def ceu_dimension(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    df,
    ceu_cap: float,
):
    """Adiciona restrição de capacidade em CEU à rota."""

    def demand(idx):
        try:
            node = manager.IndexToNode(idx)
            if 0 <= node < len(df):
                return int(df.ceu_std.iloc[node])
        except Exception as e:
            print(f"⚠️ Erro ao calcular demanda CEU para idx={idx}: {e}")
        return 0  # fallback seguro

    demand_cb = routing.RegisterUnaryTransitCallback(demand)
    routing.AddDimensionWithVehicleCapacity(
        demand_cb,
        0,  # nenhum slack
        [int(ceu_cap)] * routing.vehicles(),  # capacidade por veículo
        True,  # capacidade cumulativa
        "Capacity",
    )
