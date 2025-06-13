# backend/solver/load.py

from typing import List
from dataclasses import dataclass, field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text


@dataclass
class Trailer:
    id: int
    registry: str = field(repr=False)
    base_city: str = field(repr=False)
    trailer_cat: int
    ceu_max: float
    ligeiro_max: int
    furgo_max: int
    rodado_max: int


async def load_trailers(sess: AsyncSession) -> List[Trailer]:
    """
    Carrega trailers ativos com suas capacidades agregadas por categoria.
    """
    q = await sess.execute(
        text(
            """
            SELECT
                t.id,
                t.registry,
                t.base_city,
                tc.id AS trailer_cat,
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
    rows = q.mappings().all()
    return [Trailer(**dict(row)) for row in rows]
