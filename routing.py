# backend/solver/routing.py
from __future__ import annotations

from typing import List, Dict, Callable, Optional, Tuple
from ortools.constraint_solver import pywrapcp
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# ―――­­­­­­­­­­­­­­­­­­­­ CONSTANTES ――― #
DEFAULT_PENALTY = 99_999  # custo p / arco quando dá erro
BIG_M = 10_000_000  # upper-bound “folgado” para dimensão DIST


# ══════════════════════════════════════════════════════════════════════════════
# 1.  LOOK-UP DE DISTÂNCIA SEGURO
# ══════════════════════════════════════════════════════════════════════════════
def safe_dist_lookup(
    dist_matrix: List[List[int]],
    manager: pywrapcp.RoutingIndexManager,
    i_idx: int,
    j_idx: int,
) -> int:
    try:
        # Proteções rápidas
        if i_idx < 0 or j_idx < 0:
            return DEFAULT_PENALTY
        if i_idx >= manager.GetNumberOfIndices() or j_idx >= manager.GetNumberOfIndices():
            return 0  # start/end → 0 km (ou DEFAULT_PENALTY)

        i = manager.IndexToNode(i_idx)
        j = manager.IndexToNode(j_idx)

        if i >= len(dist_matrix) or j >= len(dist_matrix):
            return DEFAULT_PENALTY

        return dist_matrix[i][j]

    except Exception as exc:
        logger.error("⛔ dist_lookup falhou: i=%s j=%s → %s", i_idx, j_idx, exc)
        return DEFAULT_PENALTY


# ══════════════════════════════════════════════════════════════════════════════
# 2.  MANAGER + MODEL + COST
# ══════════════════════════════════════════════════════════════════════════════
def build_routing_model(
    n_nodes: int,
    n_vehicles: int,
    starts: List[int],
    ends: List[int],
    dist_matrix: List[List[int]],
) -> tuple[pywrapcp.RoutingIndexManager, pywrapcp.RoutingModel]:
    """
    Cria RoutingIndexManager + RoutingModel já com o custo-arco = distância.
    """
    manager = pywrapcp.RoutingIndexManager(n_nodes, n_vehicles, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    # ­callback de distância
    cb_idx = routing.RegisterTransitCallback(
        lambda i, j: safe_dist_lookup(dist_matrix, manager, i, j)
    )
    routing.SetArcCostEvaluatorOfAllVehicles(cb_idx)
    return manager, routing


# ══════════════════════════════════════════════════════════════════════════════
# 3.  DEMAND (capacidade) POR CATEGORIA
# ══════════════════════════════════════════════════════════════════════════════
def create_demand_callbacks(
    df: pd.DataFrame,
    manager: pywrapcp.RoutingIndexManager,
    routing: pywrapcp.RoutingModel,
    depot_indices: List[int],
) -> Tuple[Dict[str, int], Dict[str, Callable[[int], int]]]:
    """
    Cria callbacks de demanda para diferentes tipos de capacidade (CEU, LIG, FUR, ROD).

    Retorna:
      - cb_indices: dict com nomes e IDs registrados no OR-Tools
      - demand_fns: dict com as funções reais de demanda para debug
    """
    cb_indices: Dict[str, int] = {}
    demand_fns: Dict[str, Callable[[int], int]] = {}
    n = len(df)

    def build_demand(kind: str) -> Callable[[int], int]:
        def demand(index: int) -> int:
            if index < 0 or index >= manager.GetNumberOfIndices():
                return 0
            try:
                node = manager.IndexToNode(index)
            except Exception as e:
                logger.error("⛔ IndexToNode falhou index=%s: %s", index, e)
                return 0

            if node in depot_indices:
                return 0

            pickup = node < n
            base = node if pickup else node - n

            if base < 0 or base >= n:
                logger.debug("🔕 Ignorando node fora do range válido: node=%s base=%s df_len=%d", node, base, len(df))
                return 0

            cat = str(df.at[base, "vehicle_category_name"]).lower() if pd.notna(df.at[base, "vehicle_category_name"]) else ""
            ceu_val = int(df.at[base, "ceu_int"]) if pd.notna(df.at[base, "ceu_int"]) and pickup else 0

            if kind == "ceu":
                return ceu_val
            if kind == "lig":
                return 0 if not pickup else (0 if "moto" in cat else 1)
            if kind == "fur":
                return 0 if not pickup else (1 if "furg" in cat else 0)
            if kind == "rod":
                return 0 if not pickup else (1 if "rodado" in cat else 0)

            return 0

        return demand

    for kind in ["ceu", "lig", "fur", "rod"]:
        fn = build_demand(kind)
        demand_fns[kind] = fn
        cb_indices[kind] = routing.RegisterUnaryTransitCallback(fn)

        if __debug__:
            logger.warning("🧪 Debug manual para callback %s", kind)
            for idx in range(manager.GetNumberOfIndices()):
                try:
                    val = fn(idx)
                    node = manager.IndexToNode(idx)
                    logger.warning("🧪 %s → idx=%d, node=%d, demand=%s", kind.upper(), idx, node, val)
                except Exception as e:
                    logger.error("⛔ Callback %s falhou para idx=%d: %s", kind, idx, e)

    return cb_indices, demand_fns



def log_base_invalid(
    df: pd.DataFrame,
    node: int,
    base: int,
    pickup: bool,
    kind: str,
    trailers: Optional[List[dict]] = None,
    vehicle_idx: Optional[int] = None,
) -> None:
    is_pickup = "pickup" if pickup else "delivery"

    ceu_val = (
        df.at[base, "ceu_int"]
        if "ceu_int" in df.columns and 0 <= base < len(df) and pd.notna(df.at[base, "ceu_int"])
        else "N/A"
    )
    matricula = (
        str(df.at[base, "matricula"])
        if "matricula" in df.columns and 0 <= base < len(df) and pd.notna(df.at[base, "matricula"])
        else "N/A"
    )
    cat = (
        str(df.at[base, "vehicle_category_name"])
        if "vehicle_category_name" in df.columns and 0 <= base < len(df) and pd.notna(df.at[base, "vehicle_category_name"])
        else "N/A"
    )
    try:
        idx_val = df.index[base]
    except Exception:
        idx_val = "?"

    logger.warning("⚠️ Base fora do intervalo: node=%s base=%s", node, base)
    logger.warning(
        "⚠️ BASE inválido [%s]: node=%d base=%d df_len=%d df.index=%s kind=%s ceu_int=%s matricula=%s cat='%s'",
        is_pickup,
        node,
        base,
        len(df),
        idx_val,
        kind.upper(),
        ceu_val,
        matricula,
        cat,
    )

    if 0 <= base < len(df):
        logger.debug("📄 Linha df.iloc[%d]:\n%s", base, df.iloc[base].to_dict())

    if trailers and vehicle_idx is not None and 0 <= vehicle_idx < len(trailers):
        logger.debug("🚛 Trailer #%d: %s", vehicle_idx, trailers[vehicle_idx])





# ══════════════════════════════════════════════════════════════════════════════
# 4.  DIMENSÕES DE CAPACIDADE
# ══════════════════════════════════════════════════════════════════════════════
def add_dimensions_and_constraints(
    routing: pywrapcp.RoutingModel,
    trailers: list[dict],
    callbacks: Dict[str, int],
) -> None:
    """
    Cria as dimensões CEU / LIG / FUR / ROD se houver capacidade > 0.
    """
    # capacidade por trailer (já em unidades inteiras)
    ceu_cap = [int(round(float(t["ceu_max"]) * 10)) for t in trailers]
    lig_cap = [int(t["ligeiro_max"] or 0) for t in trailers]
    fur_cap = [int(t["furgo_max"] or 0) for t in trailers]
    rod_cap = [int(t["rodado_max"] or 0) for t in trailers]

    for name, caps, key in [
        ("CEU", ceu_cap, "ceu"),
        ("LIG", lig_cap, "lig"),
        ("FUR", fur_cap, "fur"),
        ("ROD", rod_cap, "rod"),
    ]:
        if all(c == 0 for c in caps):  # nenhum trailer tem esta capacidade
            continue
        cb_idx = callbacks[key]

        logger.debug("↳ Dimensão %-3s caps=%s", name, caps)
        routing.AddDimensionWithVehicleCapacity(
            cb_idx,  # transit callback
            0,  # slack
            caps,  # capacidade por veículo
            True,  # start cumul = 0
            name,
        )
        
        
        
def selecionar_subconjunto_compativel(df: pd.DataFrame, trailer_cap_ceu: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Seleciona o maior subconjunto de veículos cuja demanda CEU somada cabe na capacidade do trailer.

    Retorna:
      - df_ok: veículos a usar nesta volta
      - df_pendentes: restantes
    """
    df = df.copy()
    df = df.sort_values(by="ceu_int", ascending=False).reset_index(drop=True)

    carga_total = 0
    indices_ok = []

    for i, row in df.iterrows():
        ceu = int(row["ceu_int"])
        if carga_total + ceu <= trailer_cap_ceu:
            carga_total += ceu
            indices_ok.append(i)
        else:
            break

    df_ok = df.loc[indices_ok].reset_index(drop=True)
    df_restante = df.drop(indices_ok).reset_index(drop=True)

    return df_ok, df_restante        

# ══════════════════════════════════════════════════════════════════════════════
# 5.  DISTÂNCIA COM PENALIZAÇÃO + LIMITE facultativo
# ══════════════════════════════════════════════════════════════════════════════
def add_distance_penalty(
    routing: pywrapcp.RoutingModel,
    manager: pywrapcp.RoutingIndexManager,
    dist_matrix: List[List[int]],  # ← agora logo a seguir a manager
    *,
    penalty_per_km: int = 1,
    max_km: int | None = None,
) -> None:
    """
    Adiciona a dimensão 'DIST' que acumula km:
      • custo-extra = penalty_per_km × global_span
      • opcionalmente impõe max_km por veículo
    """
    DIM = "DIST"

    cb_idx = routing.RegisterTransitCallback(
        lambda i, j: safe_dist_lookup(dist_matrix, manager, i, j)
    )

    routing.AddDimension(
        cb_idx,
        0,  # slack
        BIG_M,  # upper bound
        True,  # start cumul at 0
        DIM,
    )

    dist_dim = routing.GetDimensionOrDie(DIM)
    dist_dim.SetGlobalSpanCostCoefficient(penalty_per_km)

    if max_km is not None:
        for v in range(routing.vehicles()):
            dist_dim.CumulVar(routing.End(v)).SetMax(max_km)
