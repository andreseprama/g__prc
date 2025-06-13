# backend/solver/optimizer/prepare_input.py
import logging
import pandas as pd
from datetime import date
from typing import Optional, Tuple, List, Dict

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from backend.solver.location_rules import fetch_city_base_map
from backend.solver.input import load_trailers
from backend.solver.optimizer.utils_df import (
    normalize_city_fields,
    calculate_ceu,
    add_base_flags,
)
from backend.solver.optimizer.trailer_routing import match_trailers_by_registry

logger = logging.getLogger(__name__)


async def prepare_input_dataframe(
    sess: AsyncSession, dia: date, matricula: Optional[str] = None
) -> Tuple[pd.DataFrame, List, dict]:
    """
    Carrega e prepara o DataFrame com serviços e trailers, aplicando validações e normalizações.

    :return: (df, trailers, base_map)
    """
    df = await load_dataframe(sess, dia)
    if df is None or df.empty:
        logger.warning(f"⚠️ Nenhum serviço encontrado para {dia}")
        return pd.DataFrame(), [], {}

    # Normalizar campos e enriquecer com CEU e base flags
    df = normalize_city_fields(df)
    df = calculate_ceu(df)
    base_map = await fetch_city_base_map(sess)
    df = add_base_flags(df, base_map)

    # Carregar trailers ativos
    trailers = await load_trailers(sess)

    # ✅ Converte para dicionários (necessário!)
    trailers = [dict(t._mapping) for t in trailers]

    if matricula:
        trailers = match_trailers_by_registry(trailers, matricula)

        if not trailers:
            logger.warning(f"❌ Nenhum trailer encontrado com matrícula {matricula}")
            return pd.DataFrame(), [], base_map

    return df, trailers, base_map


async def load_dataframe(sess: AsyncSession, dia: date) -> Optional[pd.DataFrame]:
    """
    Extrai os serviços elegíveis até o dia informado.
    """
    sql = text(
        """
        SELECT  id,
                campos->'load_city'->>'description'    AS load_city_description,
                campos->'unload_city'->>'description'  AS unload_city_description,
                campos->>'expected_delivery_date'       AS expected_delivery_date,
                campos->>'expected_delivery_date_manual'AS expected_delivery_date_manual,
                campos->'vehicle_category'->>'name'     AS vehicle_category_name,
                campos->'insurance_company'->>'short_name' AS insurance_company_short_name
        FROM    ids_monitorados
        WHERE   campos->'state'->>'id' IN ('P','PA','A','S','AM')
          AND   campos->'service_category'->>'id' IN (
                '8','10','12','13','19','25','27','28','29',
                '33','37','48','50','85','86'
          )
          AND   COALESCE(
                  (campos->>'expected_delivery_date_manual')::date,
                  (campos->>'expected_delivery_date')::date
                ) <= :dia
        """
    )

    result = await sess.execute(sql, {"dia": dia})
    rows = result.fetchall()
    if not rows:
        logger.warning("Nenhum serviço elegível encontrado para %s", dia)
        return None

    df = pd.DataFrame(rows, columns=list(result.keys()))
    df["due"] = pd.to_datetime(
        df.expected_delivery_date_manual.fillna(df.expected_delivery_date)
    )
    df["ins_short"] = df.insurance_company_short_name.fillna("").str.upper().str[:4]

    return df
