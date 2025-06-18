from typing import List, Tuple, Dict
import pandas as pd
import logging
from backend.solver.city_mapping import get_coords, haversine_km

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
        logger.warning("🚛 Trailer %d: ceu_max=%s → cap_int=%d", i, t.get("ceu_max"), cap)

    group_cols = ["id", "registry"] if "registry" in df.columns else ["id"]
    grouped = df.groupby(group_cols)

    service_blocks = []
    for group_key, group in grouped:
        total_ceu = group["ceu_int"].sum()
        service_blocks.append({
            "service_reg": group["service_reg"].iloc[0],
            "df": group,
            "ceu": total_ceu,
            "base": group["scheduled_base"].iloc[0] if "scheduled_base" in group else ""
        })

    service_blocks.sort(key=lambda s: -s["ceu"])

    used_services = []
    used_trailer_idxs = set()
    alocacoes_por_trailer: Dict[int, dict] = {}

    bases = df["scheduled_base"].dropna().unique()

    for base in bases:
        logger.info(f"📍 Alocando para base: {base}")
        blocos_base = [b for b in service_blocks if b["base"] == base]
        trailers_base = [t for t in trailer_caps if t["base_city"] == base]

        if not trailers_base:
            logger.warning(f"🚫 Nenhum trailer disponível na base {base}")
            continue

        for block in blocos_base:
            for trailer in trailers_base:
                usado = trailer["cap"] - trailer["restante"]
                ocupacao = (usado / trailer["cap"] * 100) if trailer["cap"] > 0 else 0
                status = "🟢 usado" if trailer["idx"] in used_trailer_idxs else "⚪ não usado"
                logger.info(f"🧮 Trailer {trailer['idx']}: ocupação = {usado}/{trailer['cap']} CEU ({ocupacao:.1f}%) {status}")
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
                logger.warning("❌ %s não coube em nenhum trailer na base %s", block["service_reg"], base)

    # fallback: tentar alocar blocos restantes em trailers de outras bases
    blocos_fallback = [b for b in service_blocks if not any(b["df"].equals(u) for u in used_services)]
    trailers_fallback = [t for t in trailer_caps if t["idx"] not in used_trailer_idxs]

    if blocos_fallback and trailers_fallback:
        logger.info("🔁 Fallback: tentando alocar blocos restantes em trailers de outras bases")
        for block in blocos_fallback:
            for trailer in sorted(trailers_fallback, key=lambda t: haversine_km(get_coords(t["base_city"]), get_coords(block["base"])) if get_coords(t["base_city"]) and get_coords(block["base"]) else float("inf")):
                if not get_coords(trailer["base_city"]) or not get_coords(block["base"]):
                    logger.warning(f"⚠️ Sem coordenadas para {trailer['base_city']} ou {block['base']}")
                    continue
                dist = haversine_km(get_coords(trailer["base_city"]), get_coords(block["base"]))
                if dist > 200:
                    logger.info(f"↪️ Ignorando trailer {trailer['idx']} (distância {dist:.1f}km > 200km)")
                    continue

                if block["ceu"] <= trailer["restante"]:
                    trailer["restante"] -= block["ceu"]
                    used_services.append(block["df"])
                    used_trailer_idxs.add(trailer["idx"])
                    alocacoes_por_trailer.setdefault(trailer["idx"], {
                        "base_city": trailer["base_city"],
                        "services": []
                    })
                    alocacoes_por_trailer[trailer["idx"]]["services"].append(block["service_reg"])
                    logger.warning("✅ [fallback] Alocado %s no trailer %d (%.1f km)", block["service_reg"], trailer["idx"], dist)
                    break
            else:
                logger.warning("❌ [fallback] %s não coube em nenhum trailer disponível", block["service_reg"])

    df_usado = pd.concat(used_services) if used_services else pd.DataFrame(columns=df.columns)

    if not df_usado.empty:
        chave = ["id", "registry"] if "registry" in df_usado.columns else ["id"]
        df_restante = df.merge(df_usado[chave], how="left", indicator=True)
        df_restante = df_restante[df_restante["_merge"] == "left_only"].drop(columns="_merge")
    else:
        df_restante = df

    trailers_usados = [trailers[i] for i in sorted(used_trailer_idxs)]

    return df_usado.reset_index(drop=True), df_restante.reset_index(drop=True), trailers_usados, alocacoes_por_trailer
