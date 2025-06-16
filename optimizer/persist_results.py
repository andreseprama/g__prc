from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Tuple, Dict, Any
import logging
import pandas as pd
from datetime import date

logger = logging.getLogger(__name__)


async def persist_routes(
    sess: AsyncSession,
    dia: date,
    df: pd.DataFrame,
    routes: List[Tuple[int, List[int]]],  # (vehicle_id, [nós do solver])
    trailer_starts: List[int],  # mantido p/ compatibilidade (não usado aqui)
    trailers: List[Dict[str, Any]],
) -> List[int]:
    """
    Persiste rotas e paragens (pickup + delivery).
    total_km fica a 0, total_ceu = soma de CEU dos pickups.
    """
    rota_ids: List[int] = []
    n_srv = len(df)

    for vehicle_id, path in routes:
        trailer = trailers[vehicle_id]

        # ——— métricas ———
        ceu = sum(df.ceu_int.iloc[node] for node in path if node < n_srv) / 10.0

        # ——— cria a rota ———
        q_rota = await sess.execute(
            text(
                """
                INSERT INTO rota (data, trailer_id, origem_idx,
                                  total_km, total_ceu)
                VALUES (:data, :trailer_id, 0,      -- origem fictícia
                        0,          -- total_km = 0
                        :total_ceu)
                RETURNING id
                """
            ),
            {
                "data": dia,
                "trailer_id": trailer["id"],
                "total_ceu": ceu,
            },
        )
        rota_id = q_rota.scalar()
        if rota_id is None:
            logger.error("❌ Falha ao criar rota para trailer %s", trailer["registry_trailer"])
            continue

        rota_ids.append(rota_id)
        # — Correção: removemos os '+' errados ——
        logger.info(
            "📝 Rota %s criada para trailer %s (CEU=%.1f)",
            rota_id,
            trailer["registry"],
            ceu,
        )

        # ——— insere as paragens (pickup + delivery) ———
        for ordem, node in enumerate(path):
            is_pickup = node < n_srv
            service_idx = node if is_pickup else node - n_srv
            service_id = int(df.iloc[service_idx]["id"])
            node_type = "PICKUP" if is_pickup else "DELIVERY"
            try:
                await sess.execute(
                    text(
                        """
                        INSERT INTO rota_parada (rota_id, ordem, service_id, node_type)
                        VALUES (:rota_id, :ordem, :service_id, :node_type)
                        """
                    ),
                    {
                        "rota_id": rota_id,
                        "ordem": ordem,
                        "service_id": service_id,
                        "node_type": node_type,
                    },
                )
            except Exception as e:
                logger.warning(
                    "⚠️ Erro ao adicionar parada (ordem=%s, node=%s) na rota %s: %s",
                    ordem,
                    node,
                    rota_id,
                    e,
                )

    await sess.commit()
    return rota_ids
