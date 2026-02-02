# ============================================================
# Projeto: Teste de Nivelamento
# Módulo: test_IC
# Autor: Reginaldo Filho (github.com/S1G3R)
# Data: 2026-02-02
# Descrição: Projeto para ingressar no Estágio
# ============================================================
import re
from pathlib import Path

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "data" / "output"
RAW_DIR = BASE_DIR / "data" / "raw"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

CONSOLIDADO_PATH = OUTPUT_DIR / "consolidado_despesas.csv"
ENRIQUECIDO_PATH = OUTPUT_DIR / "despesas_validas_enriquecidas.csv"

# URL base do diretório de operadoras ativas (ajuste se o nome do arquivo mudar)
OPERADORAS_BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"


# ------------------------
# Validação de CNPJ
# ------------------------

def limpar_cnpj(cnpj: str) -> str:
    """Remove tudo que não for dígito."""
    return re.sub(r"\D", "", str(cnpj))


def cnpj_valido(cnpj: str) -> bool:
    """
    Valida CNPJ (formato e dígitos verificadores).
    Estratégia: implementar o algoritmo padrão de CNPJ.
    """
    cnpj = limpar_cnpj(cnpj)
    if len(cnpj) != 14:
        return False

    if cnpj == cnpj[0] * 14:
        return False

    def calcular_digito(cnpj_parcial: str) -> int:
        pesos = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        soma = sum(int(d) * pesos[i + (len(pesos) - len(cnpj_parcial))]
                   for i, d in enumerate(cnpj_parcial))
        resto = soma % 11
        return 0 if resto < 2 else 11 - resto

    # primeiros 12 dígitos
    d1 = calcular_digito(cnpj[:12])
    # primeiros 13 dígitos
    d2 = calcular_digito(cnpj[:12] + str(d1))

    return cnpj[-2:] == f"{d1}{d2}"


# ------------------------
# Baixar / carregar operadoras ativas
# ------------------------

def baixar_csv_operadoras_ativas() -> Path:
    """
    Baixa o CSV de operadoras ativas da ANS para data/raw.
    Aqui fazemos de forma simples: assumimos que existe um arquivo .csv no diretório
    e escolhemos o primeiro que encontrar.
    Em um cenário real, você pode fixar o nome ou escolher o mais recente.
    """
    resp = requests.get(OPERADORAS_BASE_URL, timeout=60)
    resp.raise_for_status()
    html = resp.text

    # Regex simples para encontrar links terminando com .csv
    import re as _re
    matches = _re.findall(r'href="([^"]+\.csv)"', html, flags=_re.IGNORECASE)
    if not matches:
        raise RuntimeError("Não encontrei arquivo .csv de operadoras ativas no diretório.")

    csv_name = matches[0]
    csv_url = OPERADORAS_BASE_URL + csv_name

    destino = RAW_DIR / csv_name
    if destino.exists():
        print(f"CSV de operadoras já existe em {destino}")
        return destino

    print(f"Baixando CSV de operadoras ativas: {csv_url}")
    r = requests.get(csv_url, timeout=120)
    r.raise_for_status()
    with open(destino, "wb") as f:
        f.write(r.content)

    return destino


def carregar_operadoras_ativas(path: Path) -> pd.DataFrame:
    """
    Carrega o CSV de operadoras ativas em um DataFrame.
    Ajuste o separador/encoding conforme necessário.
    """
    df = pd.read_csv(path, sep=";", encoding="latin1")
    # Ajuste: padronizar nome da coluna de CNPJ (depende do arquivo real)
    # Exemplo: pode ser "CNPJ", "CNPJ_Base", "CNPJ da Operadora", etc.
    cols_lower = {c.lower(): c for c in df.columns}
    # heurísticas
    cnpj_col = next(
        (orig for lower, orig in cols_lower.items() if "cnpj" in lower),
        None
    )
    if cnpj_col is None:
        raise RuntimeError("Não encontrei coluna de CNPJ no CSV de operadoras ativas.")

    df["CNPJ"] = df[cnpj_col].astype(str).map(limpar_cnpj)

    # tentar achar colunas RegistroANS, Modalidade, UF
    reg_col = next(
        (orig for lower, orig in cols_lower.items() if "registro" in lower and "ans" in lower),
        None
    )
    mod_col = next(
        (orig for lower, orig in cols_lower.items() if "modalidade" in lower),
        None
    )
    uf_col = next(
        (orig for lower, orig in cols_lower.items() if lower in ("uf", "estado")),
        None
    )

    # renomear para padrão
    rename_map = {}
    if reg_col:
        rename_map[reg_col] = "RegistroANS"
    if mod_col:
        rename_map[mod_col] = "Modalidade"
    if uf_col:
        rename_map[uf_col] = "UF"

    df = df.rename(columns=rename_map)

    # Se houver múltiplos registros por CNPJ, vamos manter apenas o primeiro
    # (trade-off: simples; você explica no README que esta foi a escolha)
    df = df.sort_values(by=["CNPJ"]).drop_duplicates(subset=["CNPJ"], keep="first")

    return df[["CNPJ", "RegistroANS", "Modalidade", "UF"]]


# ------------------------
# Pipeline desafio 2
# ------------------------

def validar_consolidado() -> pd.DataFrame:
    """
    Lê consolidado_despesas.csv e aplica validações:
      - CNPJ válido
      - ValorDespesas > 0
      - RazaoSocial não vazia
    """
    if not CONSOLIDADO_PATH.exists():
        raise FileNotFoundError(f"Não encontrei {CONSOLIDADO_PATH}. Rode o desafio 1 antes.")

    df = pd.read_csv(CONSOLIDADO_PATH, encoding="utf-8")

    # Padronizar CNPJ
    df["CNPJ"] = df["CNPJ"].astype(str).map(limpar_cnpj)

    # Validar CNPJ
    df["CNPJ_valido"] = df["CNPJ"].apply(cnpj_valido)

    # Limpar RazaoSocial
    df["RazaoSocial"] = df["RazaoSocial"].astype(str).str.strip()

    # Garantir numérico em ValorDespesas
    df["ValorDespesas"] = pd.to_numeric(df["ValorDespesas"], errors="coerce")

    # Exemplo de estratégia:
    # manter apenas registros 100% válidos, descartando o resto
    df_validos = df[
        (df["CNPJ_valido"]) &
        (df["ValorDespesas"].notna()) &
        (df["ValorDespesas"] > 0) &
        (df["RazaoSocial"] != "")
    ].copy()

    # Opcional: remover coluna auxiliar
    df_validos = df_validos.drop(columns=["CNPJ_valido"])

    return df_validos


def enriquecer_com_cadastro(df_despesas: pd.DataFrame) -> pd.DataFrame:
    """
    Faz join com dados cadastrais de operadoras ativas.
    """
    csv_operadoras = baixar_csv_operadoras_ativas()
    cadastro = carregar_operadoras_ativas(csv_operadoras)

    # left join: mantém todas as despesas, mesmo sem cadastro
    df_join = df_despesas.merge(
        cadastro,
        on="CNPJ",
        how="left",
        validate="m:1"  
        # garante que cada CNPJ de despesas casa com no máximo 1 no cadastro
    )

    # Flag para registros sem cadastro
    df_join["SemCadastro"] = df_join["RegistroANS"].isna()

    return df_join


def main():
    print("Validando consolidado_despesas.csv...")
    df_validos = validar_consolidado()
    print(f"Registros válidos: {len(df_validos)}")

    print("Enriquecendo com dados cadastrais...")
    df_enriquecido = enriquecer_com_cadastro(df_validos)

    df_enriquecido.to_csv(ENRIQUECIDO_PATH, index=False, encoding="utf-8")
    print(f"Arquivo enriquecido salvo em: {ENRIQUECIDO_PATH}")


if __name__ == "__main__":
    main()
