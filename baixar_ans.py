# ============================================================
# Projeto: Teste de Nivelamento
# Módulo: test_IC
# Autor: Reginaldo Filho (github.com/S1G3R)
# Data: 2026-02-02
# Descrição: Projeto para ingressar no Estágio
# ============================================================
import os
import requests
import urllib3
from pathlib import Path

# Configurações
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# LISTA FIXA DE LINKS 
URLS_ALVO = [
    # Ano 2025 - 1º Trimestre
    {
        "url": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/1T2025.zip",
        "nome_salvar": "2025_01_demonstracoes.zip" 
    },
    # Ano 2025 - 2º Trimestre
    {
        "url": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/2T2025.zip",
        "nome_salvar": "2025_02_demonstracoes.zip"
    },
    # Ano 2025 - 3º Trimestre
    {
        "url": "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/3T2025.zip",
        "nome_salvar": "2025_03_demonstracoes.zip"
    }
]

def main():
    print(">>> INICIANDO DOWNLOAD DIRETO (VIA PIPELINE) <<<")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    for item in URLS_ALVO:
        url = item["url"]
        nome_arquivo = item["nome_salvar"]
        destino = RAW_DIR / nome_arquivo

        print(f"\nTentando baixar: {nome_arquivo}")
        print(f"URL: {url}")

        if destino.exists():
            print(" -> Arquivo já existe. Pulando.")
            continue

        try:
            # Tenta baixar
            r = requests.get(url, headers=headers, stream=True, verify=False, timeout=120)
            
            # Se der erro 404 (não achou), avisa
            if r.status_code == 404:
                print(" -> ERRO 404: O link mudou ou o arquivo não existe nesta URL exata.")
                continue
            
            r.raise_for_status()
            
            with open(destino, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(" -> SUCESSO! Download concluído.")
            
        except Exception as e:
            print(f" -> ERRO: {e}")

if __name__ == "__main__":
    main()