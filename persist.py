from datetime import date
from typing import List, Tuple

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.rota import Rota, RotaParada
from backend.solver.load import (
    Trailer,
)  # Certifique-se de importar Trailer se estiver em outro módulo
import logging


async def persist_routes(
    sess: AsyncSession,
    dia: date,
    df: pd.DataFrame,
    routes: List[Tuple[int, List[int]]],
    trailer_starts: List[int],  # índices das bases no modelo
    trailers: List[Trailer],
) -> List[int]:
    rota_ids: List[int] = []

    if df.empty:
        logging.warning(
            "⚠️ DataFrame de serviços está vazio. Nenhuma rota será persistida."
        )
        return []

    total_services = len(df)

    for vehicle_id, path in routes:
        if len(path) <= 1:
            logging.info(f"🚫 Ignorando veículo {vehicle_id}: rota vazia ou trivial.")
            continue

        trailer_id = trailers[vehicle_id].id
        depot_node = trailer_starts[vehicle_id]
        logging.info(
            f"📌 Persistindo rota para trailer {trailer_id} com {len(path)} nós."
        )

        rota = Rota(data=dia, trailer_id=trailer_id, peso_versao=dia)
        sess.add(rota)

        for ordem, node in enumerate(path):
            if node in trailer_starts:
                logging.debug(f"↩️ Ignorado node de base (index {node}).")
                continue  # ignora base

            # Índice de serviço
            srv_idx = node if node < total_services else node - total_services

            if srv_idx >= total_services:
                logging.warning(
                    f"⚠️ Ignorando node {node}: índice {srv_idx} fora de alcance (max {total_services - 1})."
                )
                continue

            node_type = "pickup" if node < total_services else "delivery"

            try:
                parada = RotaParada(
                    rota=rota,
                    ordem=ordem,
                    service_id=int(df.id.iat[srv_idx]),
                    node_type=node_type,
                    orig_load_city=(
                        str(df.orig_load_city.iat[srv_idx])
                        if "orig_load_city" in df
                        else None
                    ),
                    orig_unload_city=(
                        str(df.orig_unload_city.iat[srv_idx])
                        if "orig_unload_city" in df
                        else None
                    ),
                )
                sess.add(parada)

            except Exception as e:
                logging.error(f"❌ Erro ao criar parada para serviço {srv_idx}: {e}")
                continue

        await sess.flush()
        rota_ids.append(rota.id)
        logging.info(f"✅ Rota ID {rota.id} persistida com {ordem + 1} paradas.")

    await sess.commit()
    logging.info(f"🎯 {len(rota_ids)} rotas persistidas com sucesso.")
    return rota_ids
