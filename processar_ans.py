# ============================================================
# Projeto: Teste de Nivelamento
# Módulo: test_IC
# Autor: Reginaldo Filho (github.com/S1G3R)
# Data: 2026-02-02
# Descrição: Projeto para ingressar no Estágio
# ============================================================
import zipfile
from pathlib import Path
import pandas as pd
import shutil

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
INTERMEDIATE_DIR = BASE_DIR / "data" / "intermediate"
OUTPUT_DIR = BASE_DIR / "data" / "output"

INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def extrair_zips():
    """Extrai todos os ZIPs de data/raw para data/intermediate."""
    if INTERMEDIATE_DIR.exists():
        shutil.rmtree(INTERMEDIATE_DIR)
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    for zip_path in RAW_DIR.glob("*.zip"):
        print(f"Extraindo {zip_path.name}...")
        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                subfolder = INTERMEDIATE_DIR / zip_path.stem
                subfolder.mkdir(parents=True, exist_ok=True)
                z.extractall(subfolder)
        except zipfile.BadZipFile:
            print(f"Erro: Arquivo zip corrompido {zip_path}")

def ler_arquivo_generico(path: Path) -> pd.DataFrame | None:
    try:
        ext = path.suffix.lower()
        if ext == ".csv":
            return pd.read_csv(path, sep=";", encoding="latin1", on_bad_lines='skip', low_memory=False)
        elif ext == ".txt":
            return pd.read_csv(path, sep=";", encoding="latin1", on_bad_lines='skip', low_memory=False)
        elif ext in (".xls", ".xlsx"):
            return pd.read_excel(path)
        return None
    except Exception as e:
        # print(f"Erro ao ler {path.name}: {e}") 
        # Silenciando erros de arquivos inuteis
        return None

def eh_arquivo_despesas(df: pd.DataFrame) -> bool:
    colunas_lower = [str(c).lower() for c in df.columns]
    palavras_chave = ["despesa", "sinistro", "evento", "vl_saldo_final"]
    return any(p in c for c in colunas_lower for p in palavras_chave)

def normalizar_df(df: pd.DataFrame, trimestre: int, ano: int) -> pd.DataFrame:
    colunas_original = [str(c) for c in df.columns]

    def encontrar_coluna(palavras_chave: list[str]) -> str | None:
        for col in colunas_original:
            nome_lower = col.lower()
            if any(p in nome_lower for p in palavras_chave):
                return col
        return None

    cnpj_col = encontrar_coluna(["cnpj", "reg_ans"])
    razao_col = encontrar_coluna(["razao", "razão", "operadora", "nome"])
    valor_col = encontrar_coluna(["vl_saldo_final", "despesa", "sinistro", "valor"])

    if not (cnpj_col and valor_col):
        return pd.DataFrame()

    df_norm = pd.DataFrame()
    df_norm["CNPJ"] = df[cnpj_col].astype(str)
    
    if razao_col:
        df_norm["RazaoSocial"] = df[razao_col].astype(str)
    else:
        df_norm["RazaoSocial"] = ""

    df_norm["Trimestre"] = trimestre
    df_norm["Ano"] = ano
    
    val_series = df[valor_col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    df_norm["ValorDespesas"] = pd.to_numeric(val_series, errors="coerce")

    return df_norm

def extrair_info_ano_trimestre(path: Path) -> tuple[int | None, int | None]:
    folder_name = path.parent.name
    partes = folder_name.split("_")
    if len(partes) >= 2 and partes[0].isdigit() and partes[1].isdigit():
        return int(partes[0]), int(partes[1])
    return None, None

def processar_arquivos():
    dfs_norm = []
    print("Iniciando varredura...")
    for path in INTERMEDIATE_DIR.rglob("*"):
        if not path.is_file(): continue
        if path.suffix.lower() not in (".csv", ".txt", ".xls", ".xlsx"): continue

        ano, tri = extrair_info_ano_trimestre(path)
        if not ano: continue

        df = ler_arquivo_generico(path)
        if df is None or df.empty: continue

        if eh_arquivo_despesas(df):
            print(f"Processando: {path.name} ({ano}/{tri})")
            df_norm = normalizar_df(df, tri, ano)
            if not df_norm.empty:
                dfs_norm.append(df_norm)

    if dfs_norm:
        print("Consolidando...")
        df_final = pd.concat(dfs_norm, ignore_index=True)
        dest = OUTPUT_DIR / "consolidado_despesas.csv"
        df_final.to_csv(dest, index=False, encoding="utf-8")
        print(f"Salvo: {dest} ({len(df_final)} registros)")
    else:
        print("Nenhum dado encontrado.")

def main():
    print(">>> 1. Extraindo Zips")
    extrair_zips()
    print(">>> 2. Lendo e Normalizando Arquivos")
    processar_arquivos()

if __name__ == "__main__":
    main()