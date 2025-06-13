# backend\solver\optimizer\persist_results.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Tuple, Dict, Any
import logging
import pandas as pd
from datetime import date
from backend.solver.utils import norm

logger = logging.getLogger(__name__)


async def persist_routes(
    sess: AsyncSession,
    dia: date,
    df: pd.DataFrame,
    routes: list[tuple[int, list[int]]],
    trailer_starts: list[int],
    trailers: list[dict[str, Any]],
    dist_matrix: list[list[int]],
    city_idx: dict[str, int],  #  NOVO  ←
) -> list[int]:
    """
    Guarda rotas / paragens + calcula total_km e total_ceu.
    """
    rota_ids: list[int] = []
    n_srv = len(df)

    # ---------------- função auxiliar -----------------
    def node_to_city_idx(node: int) -> int:
        """Converte nº de nó (pickup/delivery) para índice de cidade."""
        if node < n_srv:  # pickup
            city = norm(df.load_city.iat[node])
        else:  # delivery
            city = norm(df.unload_city.iat[node - n_srv])
        return city_idx[city]

    # --------------------------------------------------
    for vehicle_id, path in routes:
        trailer = trailers[vehicle_id]
        base_city_idx = trailer_starts[vehicle_id]  # índice da base (= start_idx)

        # ------- total_km -------------
        km = 0
        prev = base_city_idx  # base → 1.º nó
        for n in path:
            cur = node_to_city_idx(n)
            km += dist_matrix[prev][cur]
            prev = cur
        km += dist_matrix[prev][base_city_idx]  # último nó → base
        km = int(round(km))

        # ------- total_ceu ------------
        ceu = sum(df.ceu_int.iloc[n] for n in path if n < n_srv) / 10.0

        # ------- INSERT rota ----------
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
                "origem_idx": base_city_idx,
                "total_km": km,
                "total_ceu": ceu,
            },
        )
        rota_id = q_rota.scalar()
        if rota_id is None:
            logger.error(f"❌ Falha ao criar rota para trailer {trailer['registry']}")
            continue

        rota_ids.append(rota_id)
        logger.info(f"📝 Rota {rota_id} criada (km={km}, CEU={ceu})")

        # ------- INSERT paragens -------
        for ordem, node in enumerate(path):
            try:
                is_pickup = node < n_srv
                base_idx = node if is_pickup else node - n_srv
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
                logger.warning(f"⚠️ Erro ao adicionar parada na rota {rota_id}: {e}")

    await sess.commit()
    return rota_ids
