# backend/solver/optimizer/cluster.py

from sklearn.cluster import KMeans
from backend.solver.distance import coordenada_real, get_coords, _norm
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def agrupar_por_cluster_geografico(df: pd.DataFrame, n_clusters: int = 5, tipo: str = "load") -> list[pd.DataFrame]:
    """
    Agrupa serviços com base em coordenadas reais de load ou unload.
    tipo: 'load' ou 'unload'
    """
    assert tipo in {"load", "unload"}, "tipo deve ser 'load' ou 'unload'"

    # Coordenadas reais respeitando scheduled_base
    real_cidades = df.apply(lambda r: coordenada_real(r, tipo), axis=1)
    norm_cidades = real_cidades.apply(_norm)

    coords = norm_cidades.apply(get_coords)
    valid_coords = coords[coords.notnull()].tolist()
    indices_validos = coords[coords.notnull()].index

    if len(valid_coords) < n_clusters:
        logger.warning(f"⚠️ Apenas {len(valid_coords)} com coordenadas → usando {len(valid_coords)} clusters")
        n_clusters = max(1, len(valid_coords))

    coords_df = pd.DataFrame(valid_coords, columns=["lat", "lng"])
    kmeans = KMeans(n_clusters=n_clusters, random_state=42).fit(coords_df)

    # Associa cluster ao DataFrame original
    df_clusterizado = df.copy()
    df_clusterizado.loc[indices_validos, "geo_cluster"] = kmeans.labels_

    # Retorna grupos não vazios
    grupos = [g for _, g in df_clusterizado.groupby("geo_cluster") if not g.empty]
    return grupos