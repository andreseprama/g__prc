# backend\solver\input.py
from datetime import date
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import logging

logger = logging.getLogger(__name__)


async def load_dataframe(sess: AsyncSession, dia: date) -> Optional[pd.DataFrame]:
    """
    Extrai os serviços elegíveis até o dia informado.
    Aplica normalização e enriquece com colunas derivadas.
    """
    sql = text(
        """
        SELECT
            id,
            campos->>'matricula' AS registry,
            campos->'load_city'->>'description' AS load_city_description,
            campos->'unload_city'->>'description' AS unload_city_description,
            campos->>'expected_delivery_date' AS expected_delivery_date,
            campos->>'expected_delivery_date_manual' AS expected_delivery_date_manual,
            campos->>'ceu' AS ceu_raw,
            campos->'insurance_company'->>'short_name' AS insurance_company_short_name,
            campos->'vehicle_category'->>'name' AS vehicle_category_name
        FROM ids_monitorados
        WHERE campos->'state'->>'id' IN ('P','PA','A','S','AM')
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
        logger.warning("Nenhum serviço elegível encontrado para %s", dia)
        return None

    df = pd.DataFrame(rows, columns=list(result.keys()))

    # Renomear para consistência
    df.rename(
        columns={
            "load_city_description": "load_city",
            "unload_city_description": "unload_city",
        },
        inplace=True,
    )

    # CEU: prioriza campo explícito, senão infere por categoria
    def compute_ceu(row) -> float:
        try:
            ceu_val = float(row.get("ceu_raw") or 0)
            if ceu_val > 0:
                return ceu_val
        except Exception:
            pass

        name = (row.get("vehicle_category_name") or "").lower()
        if "moto" in name:
            return 0.3
        if "furg" in name or "rodado" in name:
            return 1.5
        return 1.0

    df["ceu"] = df.apply(compute_ceu, axis=1)

    # Data final esperada
    df["due"] = pd.to_datetime(
        df.expected_delivery_date_manual.fillna(df.expected_delivery_date),
        errors="coerce",
    )

    # Código curto da seguradora
    df["ins_short"] = df.insurance_company_short_name.fillna("").str.upper().str[:4]

    return df


async def load_trailers(sess: AsyncSession) -> list:
    """
    Carrega trailers ativos com capacidades e categoria.
    """
    result = await sess.execute(
        text(
            """
            SELECT
                t.id,
                t.registry,
                t.base_city,
                tc.id AS cat_id,
                tc.ceu_max,
                tc.ligeiro_max,
                tc.furgo_max,
                tc.rodado_max
            FROM trailer t
            JOIN truck_category tc ON tc.id = t.cat_id
            WHERE t.ativo = TRUE
            """
        )
    )
    return list(result.fetchall())


async def load_constraint_weights(sess: AsyncSession) -> dict[str, float]:
    """
    Carrega pesos atuais das constraints.
    """
    sql = text(
        """
        SELECT cd.cod, cw.valor
        FROM constraint_weight cw
        JOIN constraint_def cd ON cd.id = cw.def_id
        WHERE cw.versao = (SELECT MAX(versao) FROM constraint_weight)
        """
    )
    result = await sess.execute(sql)
    return {row.cod: float(row.valor) for row in result.fetchall()}
