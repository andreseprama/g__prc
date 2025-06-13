# backend/solver/optimizer/run_optimizer.py

from datetime import date
from typing import Optional, List, Dict
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

# OR-Tools
from ortools.constraint_solver import pywrapcp  # type: ignore

# Funções utilitárias centralizadas
from backend.solver.utils import norm, build_int_distance_matrix, extract_routes
from backend.solver.geocode import fetch_and_store_city

from .prepare_input import prepare_input_dataframe
from .constraints import apply_all_constraints
from .solve_model import solve_with_params
from .persist_results import persist_routes

logger = logging.getLogger(__name__)


async def optimize(
    sess: AsyncSession,
    dia: date,
    matricula: Optional[str] = None,
    categoria_filtrada: Optional[List[str]] = None,
) -> List[int]:
    """
    1) Carrega dados
    2) Filtra categoria (se houver)
    3) Extrai e normaliza apenas as cidades usadas
    4) Carrega coords da BD
    5) Constrói matriz de distâncias (inteiros)
    6) Monta modelo OR-Tools
    7) Aplica constraints, resolve e persiste resultados
    """
    # 1) dados de entrada
    df, trailers, base_map = await prepare_input_dataframe(sess, dia, matricula)
    if df.empty:
        logger.warning("⚠️ Nenhum serviço disponível para otimização.")
        return []
    if not trailers:
        logger.warning("⚠️ Nenhum trailer ativo disponível.")
        return []

    # 2) filtro por categoria
    if categoria_filtrada:
        from .trailer_routing import filter_services_by_category

        df = filter_services_by_category(df, categoria_filtrada, base_map)
        if df.empty:
            logger.warning("⚠️ Nenhum serviço após filtro de categoria.")
            return []

    # 3) extrai + normaliza cidades
    raw_cities = (
        df["load_city"].dropna().astype(str).tolist()
        + df["unload_city"].dropna().astype(str).tolist()
        + [t["base_city"] for t in trailers if t.get("base_city")]
    )
    seen = set()
    locations: List[str] = []
    for city in raw_cities:
        c = norm(city)
        if c and c not in seen:
            seen.add(c)
            locations.append(c)
    logger.info(f"📍 Cidades usadas: {locations}")

    # ——— 4) Carrega coords existentes na BD ———
    q = await sess.execute(
        text(
            """
            SELECT city_norm   AS norm_name
                 , latitude    AS lat
                 , longitude   AS lon
              FROM public.city_coords
             WHERE city_norm = ANY(:locs)
            """
        ),
        {"locs": locations},
    )
    rows = q.fetchall()
    coords_map: Dict[str, tuple[float, float]] = {
        r.norm_name: (r.lat, r.lon) for r in rows
    }
    logger.debug(f"✅ Carreguei {len(coords_map)} coords da BD")

    # ——— 5) Preenche faltantes via TomTom ———
    missing = set(locations) - set(coords_map.keys())
    logger.debug(f"🛠️ Faltam coords para: {missing}")
    for city in missing:
        logger.info(f"🌐 Geocoding para '{city}'")
        await fetch_and_store_city(sess, city)

        # lê de volta do banco para garantir tudo
        result = await sess.execute(
            text(
                """
                SELECT latitude, longitude
                  FROM public.city_coords
                 WHERE city_norm = :city
                """
            ),
            {"city": city},
        )
        row = result.first()
        if not row:
            raise RuntimeError(f"Não encontrou coords para {city} após INSERT")
        coords_map[city] = (row.latitude, row.longitude)
        logger.debug(f"🗺️ Agora '{city}' → {coords_map[city]}")

    # ——— 6) Monta matriz de distâncias (km inteiros) ———
    dist_matrix = build_int_distance_matrix(locations, coords_map)
    logger.debug(
        f"➡️ Matriz de distâncias construída: {len(dist_matrix)}x{len(dist_matrix)}"
    )

    # ——— 7) Segue o OR-Tools normalmente… ———
    # Filtra trailers sem base_city para evitar inconsistências
    trailers = [t for t in trailers if t.get("base_city")]
    if not trailers:
        logger.warning("⚠️ Nenhum trailer com base_city definido.")
        return []

    n_nodes = len(locations)
    n_veh = len(trailers)
    city_idx = {city: idx for idx, city in enumerate(locations)}
    starts = [city_idx[norm(t["base_city"])] for t in trailers]
    ends = starts[:]
    assert (
        len(starts) == n_veh
    ), f"Vehicle count mismatch: n_veh={n_veh}, starts={len(starts)}"

    manager = pywrapcp.RoutingIndexManager(n_nodes, n_veh, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    def cost_cb(from_idx: int, to_idx: int) -> int:
        # start/end → custo 0
        if routing.IsStart(from_idx) or routing.IsEnd(from_idx):
            return 0
        if routing.IsStart(to_idx) or routing.IsEnd(to_idx):
            return 0

        # índices sentinela (fora do espaço de cidades)
        if from_idx >= manager.Size() or to_idx >= manager.Size():
            return 0

        try:
            fn = manager.IndexToNode(from_idx)
            tn = manager.IndexToNode(to_idx)

            # se por alguma razão escapou-nos um valor inválido
            if fn >= len(dist_matrix) or tn >= len(dist_matrix):
                return 0  # ← NADA de logging aqui

            return dist_matrix[fn][tn]
        except Exception:
            return 0  # ← idem, silencioso

    transit_cb = routing.RegisterTransitCallback(cost_cb)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_cb)

    # 7) pesos das constraints
    q2 = await sess.execute(
        text(
            """
            SELECT cd.cod, cw.valor
              FROM constraint_weight cw
              JOIN constraint_def cd ON cd.id = cw.def_id
             WHERE cw.versao = (SELECT MAX(versao) FROM constraint_weight)
            """
        )
    )
    rows2 = q2.fetchall()
    if not rows2:
        logger.warning("⚠️ Nenhum peso de restrição encontrado — usando pesos padrão.")
        # default values as floats
        weights: Dict[str, float] = {
            "INTERNO_LOW_PEN": 10.0,
            "PENALIDADE_DIST_KM": 3.0,
            "MAX_DIST_POR_TRAILER": 400.0,
        }
    else:
        weights = {r.cod: float(r.valor) for r in rows2}

    # # 8) aplica constraints
    try:
        apply_all_constraints(
            routing=routing,
            manager=manager,
            df=df,
            trailers=trailers,
            n_services=len(df),
            depot_indices=starts,
            distance_matrix=dist_matrix,
            constraint_weights=weights,
            enable_pickup_pairs=False,  # activa só o que precisares
        )
    except Exception as e:
        logger.exception(f"❌ Falha ao aplicar constraints: {e}")
        return []

    # 9) resolve
    try:
        solution = solve_with_params(routing, manager)
        if solution is None:
            logger.warning("⚠️ Nenhuma solução viável encontrada.")
            return []
    except Exception as e:
        logger.exception(f"❌ Erro durante resolução: {e}")
        return []

    # 10) extrai + persiste
    try:
        routes = extract_routes(
            routing, manager, solution, n_services=len(df), debug=True
        )
        rota_ids = await persist_routes(
            sess,
            dia,
            df,
            routes,
            trailer_starts=starts,
            trailers=trailers,
            dist_matrix=dist_matrix,  #  NOVO  ←
            city_idx=city_idx,  #  NOVO  ←
        )
    except Exception as e:
        logger.exception(f"❌ Falha ao persistir rotas: {e}")
        return []

    logger.info(f"✅ Persistidas {len(rota_ids)} rotas com sucesso.")
    return rota_ids
