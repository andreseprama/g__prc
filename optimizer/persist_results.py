# backend\solver\optimizer\persist_results.py
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
    routes: list[tuple[int, list[int]]],
    trailer_starts: list[int],
    trailers: list[dict[str, Any]],
    dist_matrix: list[list[int]],  #  NOVO  ←
) -> list[int]:
    """
    Guarda no banco as rotas otimizadas e suas paradas.
    Retorna os IDs das novas rotas persistidas.
    """
    rota_ids: list[int] = []

    for vehicle_id, path in routes:
        trailer = trailers[vehicle_id]
        start_idx = trailer_starts[vehicle_id]

        # --- total_km -------------------------------------------------
        km = 0
        for a, b in zip(path, path[1:]):
            km += dist_matrix[a][b]
        km = int(round(km))  # opcional: arredondar

        # --- total_ceu -----------------------------------------------
        ceu = (
            sum(df.ceu_int.iloc[node] for node in path if node < len(df)) / 10.0
        )  # volta a ser float (ex.: 3.6 CEU)

        # --------------------------------------------------------------
        # Criação da rota
        q_rota = await sess.execute(
            text(
                """
                INSERT INTO rota (data, trailer_id, origem_idx,
                                  total_km, total_ceu)
                VALUES (:data, :trailer_id, :origem_idx,
                        :total_km, :total_ceu)
                RETURNING id
                """
            ),
            {
                "data": dia,
                "trailer_id": trailer["id"],
                "origem_idx": start_idx,
                "total_km": km,  # ← NOVO
                "total_ceu": ceu,  # ← NOVO
            },
        )
        rota_id = q_rota.scalar()

        if rota_id is None:
            logging.error(f"❌ Falha ao criar rota para trailer {trailer['registry']}")
            continue

        rota_ids.append(rota_id)

        logging.info(f"📝 Criada rota ID {rota_id} para trailer {trailer['registry']}")

        for ordem, node in enumerate(path):
            try:
                n_srv = len(df)
                is_pickup = int(node) < n_srv
                base_idx = int(node) if is_pickup else int(node) - n_srv
                if base_idx >= n_srv or base_idx < 0:
                    continue

                service_id = int(df.iloc[base_idx]["id"])
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
            except Exception as e:
                logging.warning(f"⚠️ Erro ao adicionar parada na rota {rota_id}: {e}")

    await sess.commit()
    return rota_ids
