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
    routes: List[Tuple[int, List[int]]],
    trailer_starts: List[int],
    trailers: List[Dict[str, Any]],
) -> List[int]:
    """
    Guarda as rotas optimizadas **sem** distâncias:
      • total_km = 0
      • total_ceu = soma dos pickups / 10
      • impede associação de um mesmo service_id a múltiplos trailers
    """
    rota_ids: List[int] = []
    n_srv = len(df)

    for vehicle_id, path in routes:
        trailer = trailers[vehicle_id]
        ceu = sum(df.ceu_int.iloc[n] for n in path if n < n_srv) / 10.0

        # Cria rota
        q = await sess.execute(
            text(
                """
                INSERT INTO rota (data, trailer_id, origem_idx,
                                  total_km, total_ceu)
                VALUES (:data, :trailer_id, 0, 0, :total_ceu)
                RETURNING id
                """
            ),
            {"data": dia, "trailer_id": trailer["id"], "total_ceu": ceu},
        )
        rota_id = q.scalar()
        if rota_id is None:
            logger.error("❌ Falha ao criar rota para trailer %s", trailer["registry"])
            continue
        rota_ids.append(rota_id)

        # Insere paragens, com verificação de colisão e trailer único
        for ordem, node in enumerate(path):
            is_pickup = node < n_srv
            base_idx = node if is_pickup else node - n_srv
            service_id = int(df.iloc[base_idx]["id"])
            node_type = "PICKUP" if is_pickup else "DELIVERY"

            # Verifica se o service_id já foi associado a outro trailer
q_check = await sess.execute(
    text("""
        SELECT r.trailer_id, rp.node_type
        FROM rota_parada rp
        JOIN rota r ON r.id = rp.rota_id
        WHERE rp.service_id = :sid
        LIMIT 1
    """),
    {"sid": service_id},
)
row = q_check.first()
if row is not None:
    trailer_existente_id, tipo_existente = row
    if trailer_existente_id != trailer["id"]:
        logger.warning(
            f"🚫 Ignorado service_id={service_id}: já associado ao trailer_id={trailer_existente_id}, atual={trailer['id']}"
        )
        continue
    elif tipo_existente == node_type:
        logger.warning(
            f"⚠️ Ignorado service_id duplicado: {service_id} já tem paragem {node_type} associada."
        )
        continue

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

    await sess.commit()
    return rota_ids
