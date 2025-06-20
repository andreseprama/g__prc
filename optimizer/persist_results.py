from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Tuple, Dict, Any, Optional
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
    df_idx_map: Optional[Dict[int, int]] = None,
) -> List[int]:
    """
    Persiste rotas e paragens (pickup + delivery).
    total_km fica a 0, total_ceu = soma de CEU dos pickups.
    Suporta mapeamento explícito df_idx_map[node] -> df_index.
    """
    rota_ids: List[int] = []
    n_srv = len(df)

    for vehicle_id, path in routes:
        trailer = trailers[vehicle_id]

        # --- cálculo CEU ---
        ceu_total = 0
        for node in path:
            if node >= n_srv:
                continue
            idx = df_idx_map.get(node, node) if df_idx_map else node
            if 0 <= idx < len(df):
                ceu_total += df.ceu_int.iloc[idx]
            else:
                logger.warning(f"⚠️ Índice CEU inválido: node={node} → idx={idx}, df_len={len(df)}")
        ceu = ceu_total / 10.0

        # --- cria a rota ---
        q_rota = await sess.execute(
            text(
                """
                INSERT INTO rota (data, trailer_id, origem_idx,
                                  total_km, total_ceu)
                VALUES (:data, :trailer_id, 0, 0, :total_ceu)
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
        logger.info(
            "📝 Rota %s criada para trailer %s (CEU=%.1f)",
            rota_id,
            trailer["registry_trailer"],
            ceu,
        )

        # --- insere as paragens e atualiza service.rota_id ---
        for ordem, node in enumerate(path):
            is_pickup = node < n_srv
            idx = df_idx_map.get(node, node) if df_idx_map else node
            if not (0 <= idx < len(df)):
                logger.warning(f"⚠️ Índice inválido ao buscar service_id: node={node} → idx={idx}, len(df)={len(df)}")
                continue
            try:
                row = df.iloc[idx]
                service_id = int(row["id"])
                node_type = "PICKUP" if is_pickup else "DELIVERY"

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

                # Atualiza o serviço com rota_id se ainda não tiver
                await sess.execute(
                    text(
                        """
                        UPDATE ids_monitorados
                        SET rota_id = :rota_id
                        WHERE id = :service_id AND rota_id IS NULL
                        """
                    ),
                    {
                        "rota_id": rota_id,
                        "service_id": service_id
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

    # ✅ Mapear services para rota_id
    if df_idx_map is None:
        df_idx_map = {i: i for i in range(len(df))}

    node_to_service = {
        df_idx_map.get(node, node): rota_id
        for rota_id, (_, path) in zip(rota_ids, routes)
        for node in path
        if node < n_srv and 0 <= df_idx_map.get(node, node) < len(df)
    }

    # 🧩 Cria nova coluna no df
    df["rota_id"] = df.index.map(node_to_service.get)

    return rota_ids
