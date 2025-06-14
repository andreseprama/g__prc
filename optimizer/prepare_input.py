# backend/solver/optimizer/prepare_input.py

import logging
from datetime import date
from typing import Optional, Tuple, List, Dict

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.solver.location_rules import fetch_city_base_map
from backend.solver.input import load_trailers
from backend.solver.optimizer.utils_df import (
    normalize_city_fields,
    calculate_ceu,
)
from backend.solver.optimizer.rules import (
    flag_return_and_base_fields,
    get_scheduled_base,
)
from backend.solver.optimizer.trailer_routing import match_trailers_by_registry

logger = logging.getLogger(__name__)


async def prepare_input_dataframe(
    sess: AsyncSession,
    dia: date,
    matricula: Optional[str] = None,
) -> Tuple[pd.DataFrame, List[dict], Dict[str, str]]:
    """
    1) Carrega serviços elegíveis até `dia`.
    2) Normaliza cidades e calcula CEU.
    3) Traz mapa city_norm → base_norm.
    4) Marca flags e identifica base agendada.
    5) Carrega trailers e filtra por matrícula (se passada).
    """
    df = await _load_dataframe(sess, dia)
    if df is None or df.empty:
        return pd.DataFrame(), [], {}

    # --- normalização e CEU ---
    df = normalize_city_fields(df)
    df = calculate_ceu(df)

    # --- mapa de concelhos → base ---
    base_map = await fetch_city_base_map(sess)

    # --- flags de retorno e base ---
    df = flag_return_and_base_fields(df, base_map)

    # --- base agendada (usando list comprehension para evitar Pylance) ---
    df["scheduled_base"] = [
        get_scheduled_base(row, base_map) for _, row in df.iterrows()
    ]

    # --- trailers ativos ---
    trailers = await load_trailers(sess)
    trailers = [dict(t._mapping) for t in trailers]

    # --- filtro opcional por matrícula ---
    if matricula:
        trailers = match_trailers_by_registry(trailers, matricula)
        if not trailers:
            logger.warning("❌ Nenhum trailer com matrícula %s", matricula)
            return pd.DataFrame(), [], base_map

    return df, trailers, base_map


async def _load_dataframe(
    sess: AsyncSession,
    dia: date,
) -> Optional[pd.DataFrame]:
    """
    Executa a query de serviços elegíveis e retorna um DataFrame cru.
    """
    sql = text(
        """
        SELECT
            id,
            campos->'load_city'->>'description'    AS load_city,
            campos->'unload_city'->>'description'  AS unload_city,
            campos->>'expected_delivery_date'       AS expected_delivery_date,
            campos->>'expected_delivery_date_manual'AS expected_delivery_date_manual,
            campos->'vehicle_category'->>'name'     AS vehicle_category_name,
            -- outros campos que você precise
            NULL AS extra  -- placeholder se quiser adicionar mais
        FROM ids_monitorados
        WHERE
            campos->'state'->>'id' IN ('P','PA','A','S','AM')
          AND campos->'service_category'->>'id' IN (
                '8','10','12','13','19','25','27','28','29',
                '33','37','48','50','85','86'
            )
          AND COALESCE(
                (campos->>'expected_delivery_date_manual')::date,
                (campos->>'expected_delivery_date')::date
            ) <= :dia
        """
    )
    result = await sess.execute(sql, {"dia": dia})
    rows = result.fetchall()
    if not rows:
        logger.warning("⚠️ Nenhum serviço encontrado para %s", dia)
        return None

    df = pd.DataFrame(rows, columns=list(result.keys()))

    df["expected_delivery_date"] = pd.to_datetime(df["expected_delivery_date"])
    df["expected_delivery_date_manual"] = pd.to_datetime(
        df["expected_delivery_date_manual"], errors="coerce"
    )
    return df
