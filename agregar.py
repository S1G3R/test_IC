# ============================================================
# Projeto: Teste de Nivelamento
# Módulo: test_IC
# Autor: Reginaldo Filho (github.com/S1G3R)
# Data: 2026-02-02
# Descrição: Projeto para ingressar no Estágio
# ============================================================
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "data" / "output"

ENRIQUECIDO_PATH = OUTPUT_DIR / "despesas_validas_enriquecidas.csv"
AGREGADAS_PATH = OUTPUT_DIR / "despesas_agregadas.csv"


def agregar_despesas():
    if not ENRIQUECIDO_PATH.exists():
        raise FileNotFoundError(
            f"Não encontrei {ENRIQUECIDO_PATH}. Rode primeiro desafio2_validar_enriquecer.py."
        )

    df = pd.read_csv(ENRIQUECIDO_PATH, encoding="utf-8")

    # Garante tipos corretos
    df["ValorDespesas"] = pd.to_numeric(df["ValorDespesas"], errors="coerce")
    df["Trimestre"] = pd.to_numeric(df["Trimestre"], errors="coerce")
    df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce")

    # Remover linhas com ValorDespesas nulo (por segurança)
    df = df[df["ValorDespesas"].notna()].copy()

    # agrupamento por RazaoSocial + UF
    group_cols = ["RazaoSocial", "UF"]

    # Total de despesas por operadora/UF
    agg = df.groupby(group_cols).agg(
        TotalDespesas=("ValorDespesas", "sum"),
        MediaDespesasTrimestre=("ValorDespesas", "mean"),
        DesvioPadraoDespesas=("ValorDespesas", "std"),
        QuantidadeRegistros=("ValorDespesas", "count")
    ).reset_index()

    # Ordenar por valor total (maior para menor)
    agg = agg.sort_values("TotalDespesas", ascending=False)

    agg.to_csv(AGREGADAS_PATH, index=False, encoding="utf-8")
    print(f"Arquivo de despesas agregadas salvo em: {AGREGADAS_PATH}")


def main():
    agregar_despesas()


if __name__ == "__main__":
    main()
