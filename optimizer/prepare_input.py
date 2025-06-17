#backend/solver/optimizer/prepare_input.py
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
    make_service_reg
)
from backend.solver.optimizer.rules import (
    flag_return_and_base_fields,
    get_scheduled_base,
)
from backend.solver.optimizer.trailer_routing import match_trailers_by_registry_trailer

from backend.solver.utils import norm

logger = logging.getLogger(__name__)


async def prepare_input_dataframe(
    sess: AsyncSession,
    dia: date,
    registry_trailer: Optional[str] = None,
    debug: bool = False,
) -> Tuple[pd.DataFrame, List[dict], Dict[str, str]]:
    """
    Prepara DataFrame e trailers para otimização:
    - Normaliza cidades
    - Calcula CEU
    - Marca is_base
    - Gera coluna service_reg
    """
    df = await _load_dataframe(sess, dia)
    if df is None or df.empty:
        return pd.DataFrame(), [], {}

    df = normalize_city_fields(df)
    df = calculate_ceu(df)

    base_map = await fetch_city_base_map(sess)
    df = flag_return_and_base_fields(df, base_map)

    df["scheduled_base"] = [
        get_scheduled_base(row, base_map) for _, row in df.iterrows()
    ]

    trailers = await load_trailers(sess)
    trailers = [dict(t._mapping) for t in trailers]

    for t in trailers:
        t["base_city"] = (t.get("base_city") or "").strip()
        t["ceu_max"] = float(t.get("ceu_max") or 6.0)

    trailers_com_base = [t for t in trailers if t.get("ativo") and t.get("base_city")]

    if debug:
        logger.debug(f"🔍 Trailers brutos: {trailers}")
        logger.debug(f"🧪 {len(trailers_com_base)} trailers ativos com base_city válidos")

    ignorados = len(trailers) - len(trailers_com_base)
    if ignorados > 0:
        logger.warning(f"⚠️ {ignorados} trailer(s) ignorado(s) por não terem base_city definida ou estarem inativos")

    trailers = trailers_com_base

    bases_unicas = sorted({norm(t["base_city"]) for t in trailers})
    logger.info(f"📍 Bases encontradas em trailers ativos: {bases_unicas}")
    logger.info(f"✅ {len(trailers)} trailers ativos com base carregados para {dia}")

    if registry_trailer:
        trailers = match_trailers_by_registry_trailer(trailers, registry_trailer)
        if not trailers:
            logger.warning("❌ Nenhum trailer com matrícula %s", registry_trailer)
            return pd.DataFrame(), [], base_map

    df = make_service_reg(df)
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
            campos->>'registry' AS matricula,
            campos->'load_city'->>'description'    AS load_city,
            campos->'unload_city'->>'description'  AS unload_city,
            campos->>'expected_delivery_date'       AS expected_delivery_date,
            campos->>'expected_delivery_date_manual'AS expected_delivery_date_manual,
            campos->'vehicle_category'->>'name'     AS vehicle_category_name
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
    df["load_city"] = df["load_city"].astype(str)
    df["unload_city"] = df["unload_city"].astype(str)
    return df


def group_similar_services(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa serviços com mesmo load/unload/scheduled_base e soma ceu_int.
    Gera também uma chave service_reg única por linha agrupada.
    """
    grouped = (
        df.groupby(["load_city", "unload_city", "scheduled_base"], dropna=False)
        .agg({
            "ceu_int": "sum",
            "id": "first",
            "matricula": "first",
            "vehicle_category_name": "first",
            "expected_delivery_date": "min",
            "expected_delivery_date_manual": "first",
            "force_return": "first",
            "load_is_base": "first",
            "unload_is_base": "first",
        })
        .reset_index()
    )
    grouped["was_grouped"] = True
    grouped = make_service_reg(grouped)
    return grouped