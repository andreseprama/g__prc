# backend/solver/optimizer/subset_selection.py

import pandas as pd
from typing import List, Tuple, Dict
import logging

logger = logging.getLogger(__name__)



def selecionar_servicos_e_trailers_compativeis(
    df: pd.DataFrame, trailers: List[dict]
) -> Tuple[pd.DataFrame, pd.DataFrame, List[dict], Dict[int, dict]]:
    if df.empty or not trailers:
        return df, df, [], {}

    trailer_caps = []
    for i, t in enumerate(trailers):
        try:
            cap = int(float(t.get("ceu_max", 0)) * 10)
        except Exception:
            cap = 0
        trailer_caps.append({
            "idx": i,
            "cap": cap,
            "restante": cap,
            "base_city": t.get("base_city", "")
        })
        logger.warning("\U0001f69b Trailer %d: ceu_max=%s → cap_int=%d", i, t.get("ceu_max"), cap)

    group_cols = ["id", "registry"] if "registry" in df.columns else ["id"]
    grouped = df.groupby(group_cols)

    service_blocks = []
    for group_key, group in grouped:
        total_ceu = group["ceu_int"].sum()
        service_blocks.append({
            "service_reg": group["service_reg"].iloc[0],
            "df": group,
            "ceu": total_ceu
        })

    service_blocks.sort(key=lambda s: -s["ceu"])

    used_services = []
    used_trailer_idxs = set()
    alocacoes_por_trailer: Dict[int, dict] = {}

    for block in service_blocks:
        for trailer in trailer_caps:
            usado = trailer["cap"] - trailer["restante"]
            ocupacao = (usado / trailer["cap"]) * 100 if trailer["cap"] > 0 else 0
            status = "\U0001f7e2 usado" if trailer["idx"] in used_trailer_idxs else "⚪ não usado"
            logger.info(f"\U0001f9ae Trailer {trailer['idx']}: ocupação = {usado}/{trailer['cap']} CEU ({ocupacao:.1f}%) {status}")
            if block["ceu"] <= trailer["restante"]:
                trailer["restante"] -= block["ceu"]
                used_services.append(block["df"])
                used_trailer_idxs.add(trailer["idx"])
                alocacoes_por_trailer.setdefault(trailer["idx"], {
                    "base_city": trailer["base_city"],
                    "services": []
                })
                alocacoes_por_trailer[trailer["idx"]]["services"].append(block["service_reg"])
                logger.warning("✅ Alocado %s no trailer %d", block["service_reg"], trailer["idx"])
                break
        else:
            logger.warning("❌ %s não coube em nenhum trailer", block["service_reg"])

    df_usado = pd.concat(used_services) if used_services else pd.DataFrame(columns=df.columns)

    if not df_usado.empty:
        chave = ["id", "registry"] if "registry" in df_usado.columns else ["id"]
        df_restante = df.merge(df_usado[chave], how="left", indicator=True)
        df_restante = df_restante[df_restante["_merge"] == "left_only"].drop(columns="_merge")
    else:
        df_restante = df

    trailers_usados = [trailers[i] for i in sorted(used_trailer_idxs)]

    return df_usado.reset_index(drop=True), df_restante.reset_index(drop=True), trailers_usados, alocacoes_por_trailer
