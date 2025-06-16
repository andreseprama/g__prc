from typing import List, Tuple
import pandas as pd


def selecionar_servicos_e_trailers_compatíveis(
    df: pd.DataFrame, trailers: List[dict]
) -> Tuple[pd.DataFrame, pd.DataFrame, List[dict]]:
    demanda_total = int(df["ceu_int"].sum())
    capacidades = [(i, int(float(t["ceu_max"]) * 10)) for i, t in enumerate(trailers)]
    capacidades.sort(key=lambda x: -x[1])  # maiores capacidades primeiro

    cap_total = sum(c for _, c in capacidades)

    if demanda_total <= cap_total:
        # Cenário A: usar só trailers suficientes
        cap_acum = 0
        usados = []
        for i, c in capacidades:
            cap_acum += c
            usados.append(i)
            if cap_acum >= demanda_total:
                break
        trailers_usados = [trailers[i] for i in usados]
        return df, pd.DataFrame(columns=df.columns), trailers_usados

    # Cenário B: selecionar subset de serviços que cabem nos trailers disponíveis
    trailers_usados = [trailers[i] for i, _ in capacidades]
    cap_total = sum(c for _, c in capacidades)
    df = df.sort_values(by="ceu_int", ascending=False).reset_index(drop=True)

    carga = 0
    indices_ok = []
    for i, row in df.iterrows():
        ceu = int(row["ceu_int"])
        if carga + ceu <= cap_total:
            carga += ceu
            indices_ok.append(i)
        else:
            break

    df_usado = df.loc[indices_ok].reset_index(drop=True)
    df_restante = df.drop(indices_ok).reset_index(drop=True)
    return df_usado, df_restante, trailers_usados
