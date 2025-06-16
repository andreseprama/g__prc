# backend/solver/optimizer/subset_selection.py

import pandas as pd
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)

def selecionar_servicos_e_trailers_compatíveis(
    df: pd.DataFrame, trailers: List[dict]
) -> Tuple[pd.DataFrame, pd.DataFrame, List[dict]]:
    demanda_total = int(df["ceu_int"].sum())

    capacidades = []
    for i, t in enumerate(trailers):
        try:
            cap = int(float(t.get("ceu_max", 0)) * 10)
        except Exception:
            cap = 0
        capacidades.append((i, cap))
        logger.warning("🚛 Trailer %d: ceu_max=%s → cap_int=%d", i, t.get("ceu_max"), cap)

    capacidades.sort(key=lambda x: -x[1])  # maiores primeiro
    cap_total = sum(c for _, c in capacidades)
    logger.warning("📦 Soma CEU dos serviços: %d | Capacidade total: %d", demanda_total, cap_total)

    if demanda_total <= cap_total:
        cap_acum = 0
        usados = []
        for i, c in capacidades:
            cap_acum += c
            usados.append(i)
            if cap_acum >= demanda_total:
                break
        trailers_usados = [trailers[i] for i in usados]
        return df, pd.DataFrame(columns=df.columns), trailers_usados

    # Caso exceda capacidade: selecionar serviços que cabem em apenas UM trailer
    idx_prim_trailer, trailer_cap_ceu = capacidades[0]
    logger.warning("🎯 Tentando encher apenas trailer #%d com cap=%d", idx_prim_trailer, trailer_cap_ceu)

    df = df.sort_values(by="ceu_int", ascending=False).reset_index(drop=True)
    carga_total = 0
    indices_ok = []

    for i, row in df.iterrows():
        ceu = int(row["ceu_int"])
        if carga_total + ceu <= trailer_cap_ceu:
            carga_total += ceu
            indices_ok.append(i)
            logger.warning("✅ adicionado idx=%d, carga acumulada=%d", i, carga_total)
        else:
            logger.warning("❌ não coube idx=%d, carga=%d + ceu=%d > cap=%d", i, carga_total, ceu, trailer_cap_ceu)
            continue

    df_usado = df.loc[indices_ok].reset_index(drop=True)
    df_restante = df.drop(indices_ok).reset_index(drop=True)

    trailers_usados = [trailers[idx_prim_trailer]] if df_usado.shape[0] > 0 else []
    return df_usado, df_restante, trailers_usados