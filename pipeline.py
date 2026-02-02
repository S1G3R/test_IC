# ============================================================
# Projeto: Teste de Nivelamento
# Módulo: test_IC
# Autor: Reginaldo Filho (github.com/S1G3R)
# Data: 2026-02-02
# Descrição: Projeto para ingressar no Estágio
# ============================================================
import sys
import time
from pathlib import Path

# Importa os módulos que foi criado
try:
    import baixar_ans
    import processar_ans
    import validar_enriquecer
    import agregar
except ImportError as e:
    print(f"ERRO CRÍTICO: Não encontrei um dos scripts: {e}")
    print("Verifique se baixar_ans.py, processar_ans.py, etc. estão nesta pasta.")
    sys.exit(1)

def banner(texto):
    print("\n" + "="*60)
    print(f" {texto}")
    print("="*60 + "\n")

def verificar_arquivo(caminho):
    path = Path(caminho)
    if path.exists():
        tamanho = path.stat().st_size / 1024 # KB
        print(f"   [OK] Arquivo gerado: {path.name} ({tamanho:.2f} KB)")
        return True
    else:
        print(f"   [ERRO] Arquivo esperado não encontrado: {path.name}")
        return False

def main():
    inicio_total = time.time()
    
    # --- ETAPA 1: INGESTÃO  ---
    banner("ETAPA 1: DOWNLOAD DOS DADOS BRUTOS (ANS)")
    try:
        baixar_ans.main()
    except Exception as e:
        print(f"Erro na Etapa 1: {e}")
        sys.exit(1)
        
    # Verificação rápida
    raw_dir = Path("data/raw")
    zips = list(raw_dir.glob("*.zip"))
    print(f"\n-> Status: {len(zips)} arquivos .zip encontrados em data/raw")
    if len(zips) == 0:
        print("Erro: Nenhum download realizado.")
        sys.exit(1)


    # --- ETAPA 2: PROCESSAMENTO  ---
    banner("ETAPA 2: EXTRAÇÃO E CONSOLIDAÇÃO")
    try:
        processar_ans.main()
    except Exception as e:
        print(f"Erro na Etapa 2: {e}")
        sys.exit(1)

    if not verificar_arquivo("data/output/consolidado_despesas.csv"):
        sys.exit(1)


    # --- ETAPA 3: ENRIQUECIMENTO  ---
    banner("ETAPA 3: VALIDAÇÃO E ENRIQUECIMENTO (CADASTRO)")
    try:
        validar_enriquecer.main()
    except Exception as e:
        print(f"Erro na Etapa 3: {e}")
        sys.exit(1)
        
    if not verificar_arquivo("data/output/despesas_validas_enriquecidas.csv"):
        sys.exit(1)


    # --- ETAPA 4: AGREGAÇÃO  ---
    banner("ETAPA 4: AGREGAÇÃO FINAL (KPIs)")
    try:
        agregar.main()
    except Exception as e:
        print(f"Erro na Etapa 4: {e}")
        sys.exit(1)

    if not verificar_arquivo("data/output/despesas_agregadas.csv"):
        sys.exit(1)

    # --- FIM ---
    tempo_total = time.time() - inicio_total
    banner(f"PIPELINE CONCLUÍDO COM SUCESSO! ({tempo_total:.2f}s)")

if __name__ == "__main__":
    main()