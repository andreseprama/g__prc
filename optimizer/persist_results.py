# backend/solver/optimizer/persist_results.py
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
    routes: list[tuple[int, list[int]]],  # (vehicle_id, [nós])
    trailer_starts: list[int],  # mantém assinatura p/ compat
    trailers: list[dict[str, Any]],
) -> list[int]:
    """
    Guarda as rotas optimizadas **sem** cálculo de distâncias.

    total_km → 0
    total_ceu → soma dos serviços atribuídos / 10
    """
    rota_ids: list[int] = []
    n_srv = len(df)

    for vehicle_id, path in routes:
        trailer = trailers[vehicle_id]

        # --- métricas -------------------------------------------------------
        km = 0  # sem distâncias
        ceu = sum(df.ceu_int.iloc[n] for n in path if n < n_srv) / 10.0

        # --------------------------------------------------------------------
        # INSERT na tabela rota
        q_rota = await sess.execute(
            text(
                """
                INSERT INTO rota (data, trailer_id, origem_idx,
                                  total_km, total_ceu)
                VALUES (:data, :trailer_id, 0,          -- origem_idx fictício
                        :total_km, :total_ceu)
                RETURNING id
                """
            ),
            {
                "data": dia,
                "trailer_id": trailer["id"],
                "total_km": km,
                "total_ceu": ceu,
            },
        )
        rota_id = q_rota.scalar()
        if rota_id is None:
            logger.error("❌ Falha ao criar rota para trailer %s", trailer["registry"])
            continue

        rota_ids.append(rota_id)
        logger.info("📝 Rota %s criada (CEU=%s)", rota_id, ceu)

        # --------------------------------------------------------------------
        # INSERT das paragens (PICKUP / DELIVERY)
        for ordem, node in enumerate(path):
            try:
                is_pickup = node < n_srv
                base_idx = node if is_pickup else node - n_srv
                service_id = int(df.iloc[base_idx]["id"])
                node_type = "PICKUP" if is_pickup else "DELIVERY"

                await sess.execute(
                    text(
                        """
                        INSERT INTO rota_parada (rota_id, ordem,
                                                 service_id, node_type)
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
                logger.warning("⚠️ Erro ao adicionar parada na rota %s: %s", rota_id, e)

    await sess.commit()
    return rota_ids
