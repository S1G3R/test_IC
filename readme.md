🏥 Pipeline ETL - Dados Abertos ANS
Este projeto consiste em um processo seletivo que foi desenvolvido em Python para extrair, transformar e analisar dados financeiros das Operadoras de Planos de Saúde no Brasil.

O sistema coleta automaticamente as Demonstrações Contábeis do site da ANS (Agência Nacional de Saúde Suplementar), processa ficheiros brutos, enriquece as informações com dados cadastrais e gera relatórios agregados.

🏗️ Arquitetura do Pipeline
O projeto segue uma arquitetura modular de ETL (Extract, Transform, Load), dividida em camadas lógicas:

Ingestão:

Script: baixar_ans.py

Função: Conecta ao servidor FTP/HTTP da ANS. Contorna bloqueios de segurança (SSL/User-Agent) e baixa os ficheiros .zip dos últimos 3 trimestres disponíveis.

Estratégia: Utiliza uma lista de tentativas diretas de URL para garantir o download mesmo se a listagem de diretórios estiver bloqueada.

Processamento:

Script: processar_ans.py

Função: Descompacta os ficheiros, identifica CSVs relevantes (heurística baseada em nomes de colunas), normaliza formatação numérica (pt-BR) e consolida milhares de registos num único ficheiro mestre (consolidado_despesas.csv).

Enriquecimento:

Script: validar_enriquecer.py

Função: Baixa o cadastro atualizado de operadoras ativas, valida CNPJs e realiza um Left Join para adicionar Razão Social, Modalidade e UF aos dados financeiros.

Agregação:

Script: agregar.py

Função: Gera KPIs finais, calculando o total de despesas e média trimestral por operadora, ordenando os dados para análise da diretoria.

Orquestrador:

Script: pipeline.py

Função: Gere a execução sequencial de todos os módulos, garantindo dependências e tratamento de erros.

📂 Estrutura de Pastas

.
├── pipeline.py             # Script principal (RODE ESTE)
├── baixar_ans.py           # Módulo de Download
├── processar_ans.py        # Módulo de Processamento
├── validar_enriquecer.py   # Módulo de Enriquecimento
├── agregar.py              # Módulo de Agregação
├── requirements.txt        # Dependências do projeto
│
├── venv/                   # Ambiente Virtual (criado localmente)
├── __pycache__/            # Cache de compilação Python (gerado automaticamente)
│
└── data/                   # Gerado automaticamente pelo pipeline
    ├── raw/                # Ficheiros .zip originais e cadastro bruto
    ├── intermediate/       # Ficheiros extraídos temporários
    └── output/             # CSVs finais tratados



# 🚀 Como Executar
Pré-requisitos
Python 3.8 ou superior.

# 1. Configurar o Ambiente Virtual (venv)
É altamente recomendado isolar as dependências do projeto.

# No Windows:

Criar o ambiente

python -m venv venv

Ativar o ambiente

venv\Scripts\activate

# No Linux/Mac:

Criar o ambiente

python3 -m venv venv

Ativar o ambiente

source venv/bin/activate

# 2. Instalação das Dependências
Com o ambiente ativado, instale as bibliotecas necessárias:

pip install -r requirements.txt

Conteúdo sugerido para requirements.txt:

pandas
requests
beautifulsoup4
urllib3
openpyxl

# 3. Rodando o Pipeline
Para executar o fluxo completo (End-to-End), basta rodar o orquestrador:

python pipeline.py

O terminal exibirá o progresso de cada etapa. Ao final, os resultados estarão na pasta data/output.

# 📊 Resultados Gerados
Na pasta data/output, encontrará três ficheiros principais:

consolidado_despesas.csv: Dados brutos unificados de todos os trimestres.

despesas_validas_enriquecidas.csv: Dados higienizados e cruzados com o cadastro oficial da ANS.

despesas_agregadas.csv: Relatório final analítico.

Exemplo de colunas: RazaoSocial, UF, Total_Despesas, Media_Trimestral.

# 🛠️ Decisões Técnicas e Desafios Superados
Bloqueio de Robôs da ANS: O site da ANS possui proteções contra scraping e certificados SSL antigos.

Solução: Implementamos headers de User-Agent simulando um navegador Chrome e desabilitamos a verificação SSL (verify=False) de forma controlada via urllib3.

Inconsistência de Nomes de Ficheiros: A ANS altera o padrão de nomeação dos ficheiros ZIP (ex: 1T2025.zip vs 2025_01_demonstracoes.zip).

Solução: O script de download utiliza uma abordagem que conecta diretamente ao destino para garantir que o ficheiro seja encontrado.

Formatação Numérica: Ficheiros CSV brasileiros usam vírgula como decimal, o que quebra leituras padrão em Pandas.

Solução: Tratamento de strings antes da conversão numérica para garantir a precisão dos cálculos financeiros.
