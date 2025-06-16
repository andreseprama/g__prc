# backend/solver/optimizer/postprocess.py
import pandas as pd
import logging
from datetime import date
from typing import Optional, List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger(__name__)


async def annotate_solution(
    df: pd.DataFrame,
    assigned_ids: List[int],
) -> pd.DataFrame:
    """
    Marca os serviços que foram alocados a uma rota vs os ignorados.
    """
    df = df.copy()
    df["assigned"] = df["id"].isin(assigned_ids)
    df["status"] = df["assigned"].map(lambda x: "OK" if x else "IGNORED")
    return df


async def summarize_coverage(df: pd.DataFrame) -> None:
    """
    Loga estatísticas da cobertura da otimização.
    """
    total = len(df)
    alocados = df["assigned"].sum() if "assigned" in df.columns else 0
    ignorados = total - alocados

    logger.info(f"📊 Serviços totais: {total}")
    logger.info(f"✅ Alocados: {alocados}")
    logger.info(f"🚫 Ignorados: {ignorados}")
    logger.info(f"🎯 Cobertura: {round(alocados / total * 100, 2) if total else 0}%")


async def get_trailers_for_run(
    sess: AsyncSession, df: pd.DataFrame, registry_trailer: Optional[str]
) -> List:
    """
    Carrega trailers e filtra por matrícula se necessário.
    """
    q = await sess.execute(
        text(
            """
            SELECT t.id, t.registry_trailer, t.base_city, tc.id as trailer_cat,
                   tc.ceu_max, tc.ligeiro_max, tc.furgo_max, tc.rodado_max
            FROM trailer t
            JOIN truck_category tc ON tc.id = t.cat_id
            WHERE t.ativo = TRUE
            """
        )
    )
    trailers = list(q.fetchall())

    if registry_trailer:
        trailers = [
            t
            for t in trailers
            if (t.registry_trailer or "").strip().upper() == registry_trailer.strip().upper()
        ]

    return trailers
