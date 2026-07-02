# 📘 Manual de Engenharia de Dados: Do Zero ao Pipeline de Produção

Este guia foi elaborado para consolidar todos os conceitos, arquiteturas e práticas aplicadas no projeto **E-Commerce ETL Pipeline**. Use este manual para estudar os fundamentos da engenharia de dados e entender as decisões técnicas de nível de produção.

---

## 🗺️ Índice
1. **Conceitos Fundamentais (OLTP vs OLAP)**
2. **A Arquitetura Medallion (Bronze ➡️ Silver ➡️ Gold)**
3. **Fase 1: Ingestão e Ingestão de APIs (Bronze)**
4. **Fase 2: Limpeza e Transformação com Pandas (Silver)**
5. **Fase 3: Qualidade de Dados (Data Quality)**
6. **Fase 4: Modelagem Dimensional (Star Schema & Gold)**
7. **Fase 5: Carga e Conectores de Nuvem (S3 & Postgres)**
8. **Orquestração de Workflows (Apache Airflow)**
9. **Desafios Práticos de Fixação**

---

## 1. Conceitos Fundamentais: OLTP vs OLAP

Antes de construir qualquer pipeline, é preciso entender os dois mundos dos bancos de dados:

### OLTP (Online Transactional Processing)
*   **Foco:** Operação diária do aplicativo.
*   **Características:** Inserções e atualizações rápidas, consultas de registros individuais (ex: "Buscar o endereço do cliente X").
*   **Estrutura:** **Normalizada** (dados divididos em muitas tabelas pequenas para evitar duplicidade).
*   **Exemplo:** O banco de dados do aplicativo de e-commerce onde as compras são registradas no carrinho de compras.

### OLAP (Online Analytical Processing) - Data Warehouse (DWH)
*   **Foco:** Análise de grandes volumes de dados históricos.
*   **Características:** Leituras massivas, agregações (somas, médias), poucos updates.
*   **Estrutura:** **Desnormalizada** (dados agrupados em poucas tabelas dimensionais para evitar JOINs lentos).
*   **Exemplo:** O Data Warehouse usado pelo time de marketing para calcular o faturamento anual por categoria.

---

## 2. A Arquitetura Medallion

Padrão moderno para estruturação de Data Lakes e Data Warehouses dividido em 3 camadas:

```
┌─────────────────┐      ┌──────────────────┐      ┌───────────────────┐
│  Camada Bronze  │ ───▶ │  Camada Silver   │ ───▶ │    Camada Gold    │
│  (Dados Brutos) │      │ (Dados Limpos)   │      │(Dados Modelados)  │
│   CSV / JSON    │      │  Parquet Snappy  │      │    Star Schema    │
└─────────────────┘      └──────────────────┘      └───────────────────┘
```

1.  **Bronze (Raw Zone):** Dados em seu estado bruto original. Nenhuma transformação é permitida aqui. Garante a reprodutibilidade histórica.
2.  **Silver (Clean Zone):** Dados higienizados, com tipos de colunas corretos, deduplicados e nulos preenchidos.
3.  **Gold (Curated Zone):** Dados agregados e estruturados em modelos dimensionais prontos para ferramentas de Business Intelligence (BI).

---

## 3. Fase 1: Ingestão de Dados (Bronze)

A primeira etapa do pipeline é ler os dados de fontes externas (APIs e CSVs).

### Ingestão de CSVs (Segurança de Encoding)
Bancos de dados exportam CSVs com delimitadores (`comma` `,` ou `semicolon` `;`) e encodings diferentes:
*   **UTF-8:** Padrão web/Linux.
*   **ISO-8859-1 (Latin-1) / CP1252:** Padrão Windows (suporta acentos em português).

Se você abrir um arquivo `Latin-1` como `UTF-8` no Python, o pipeline quebrará com o erro `UnicodeDecodeError`. Para evitar isso, usamos a biblioteca `chardet` para detectar o encoding dinamicamente:

```python
import chardet
from pathlib import Path

def detect_encoding(file_path: Path) -> str:
    with open(file_path, "rb") as f:
        raw_data = f.read(20000) # Lê os primeiros 20KB
    result = chardet.detect(raw_data)
    return result.get("encoding", "utf-8")
```

### Ingestão de APIs REST (Retry com Backoff Exponencial)
APIs sofrem oscilações de internet. Usar apenas `requests.get()` sem tratamento de erro é uma má prática. Implementamos retentativas com espera exponencial (o script aguarda mais tempo a cada falha) para não sobrecarregar o servidor:

```python
import time
import requests

def extract_with_retry(url: str, retries: int = 3, backoff: float = 2.0):
    attempt = 0
    while attempt <= retries:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            attempt += 1
            if attempt > retries:
                raise e
            sleep_time = backoff ** attempt
            time.sleep(sleep_time)
```

---

## 4. Fase 2: Limpeza e Transformação com Pandas (Silver)

Na camada Silver, o Pandas é nosso principal aliado para limpar a "sujeira" dos dados:

### A. Deduplicação
Remover registros duplicados com `.drop_duplicates()`. Devemos prestar atenção à ordem dos dados para manter o registro mais recente (`keep="first"` ou `keep="last"`).
```python
# Remove duplicados baseados no email, mantendo o primeiro registro encontrado
df = df.drop_duplicates(subset=["email"], keep="first")
```

### B. Tratamento de Valores Nulos (Nulls/NaNs)
Três estratégias para colunas nulas:
1.  **Preenchimento de Valor Padrão (Texto):** Substituir nulos por termos informativos.
    ```python
    df["phone"] = df["phone"].fillna("N/A")
    ```
2.  **Manutenção do Nulo (Numérico):** Manter nulos em colunas como `desconto` ou `rating` para não estragar médias.
3.  **Remoção de Linhas (Chaves Obrigatórias):**
    ```python
    df = df.dropna(subset=["customer_id"])
    ```

### C. Normalização de Strings
Padronizar textos (letras maiúsculas/minúsculas e espaços em branco invisíveis):
```python
# Remove espaços no início/fim e converte para minúsculo
df["email"] = df["email"].str.strip().str.lower()

# Converte nomes próprios para o formato Título (ex: joão silva -> João Silva)
df["name"] = df["name"].str.strip().str.title()
```

---

## 5. Fase 3: Qualidade de Dados (Data Quality)

Antes de carregar o Data Warehouse, validamos regras de negócio críticas.

```python
# Exemplo de verificação de completude (Percentual de nulos admissível)
def check_completeness(df, column, threshold=0.98):
    non_null_count = df[column].notnull().sum()
    pct = non_null_count / len(df)
    return pct >= threshold  # Retorna True se atingiu a meta de completude
```

As falhas de qualidade se dividem em:
*   **Falhas Críticas (Hard Alerts):** Duplicados em chaves primárias (IDs). O pipeline deve parar imediatamente.
*   **Falhas Leves (Soft Alerts):** Emails ausentes acima do esperado. O pipeline gera um aviso/log, mas continua.

---

## 6. Fase 4: Modelagem Dimensional (Star Schema & Gold)

O **Star Schema** é a estrutura mais otimizada para análise. Dividimos os dados em:

*   **Tabela Fato (fact_orders):** Guarda "o que aconteceu" e métricas (quantidade, preço, frete).
*   **Tabelas Dimensão (dim_customer, dim_product, dim_date):** Guardam os dados de contexto.

### Chaves Substitutas (Surrogate Keys) vs Chaves Naturais
Em bancos transacionais, usamos UUIDs (`cust-10293`). No DWH, criamos uma chave numérica inteira interna sequencial (`customer_key` = `1`, `2`, `3...`) para ligar as tabelas. Joins por chaves inteiras são muito mais rápidos que joins por strings.

### Slowly Changing Dimensions (SCD) Tipo 2
Usado para rastrear mudanças históricas de atributos dos clientes (ex: mudança de endereço).
*   Se o cliente mudar de endereço, em vez de sobrescrever a linha, nós inserimos uma **segunda linha** no DWH com o novo endereço, novas datas de validade e uma nova `customer_key`, mantendo o mesmo `customer_id` de origem.

```text
Tabela dim_customer:
| customer_key | customer_id | Nome   | Estado | valid_from | valid_to   | is_current |
|:---:|:---:|:---|:---|:---|:---|:---|
| 1            | c-777       | Carlos | SP     | 2024-01-01 | 2025-05-31 | False      |
| 10           | c-777       | Carlos | RJ     | 2025-06-01 | 9999-12-31 | True       |
```

### O Poder da `dim_date`
Em vez de fazermos queries SQL complexas para descobrir o dia da semana ou se era feriado a partir de uma data textual, criamos uma tabela onde cada data possui suas propriedades pré-calculadas (chave numérica `20260701` representa `2026-07-01`):

```sql
-- Query simples para descobrir vendas em feriados usando Star Schema
SELECT d.is_holiday, SUM(f.total_amount)
FROM gold.fact_orders f
JOIN gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.is_holiday;
```

---

## 7. Fase 5: Carga e Conectores de Nuvem (S3 & Postgres)

Para salvar os dados finais:

### A. AWS S3 (boto3)
Utilizado para armazenar arquivos em massa de forma barata (Datalake). Salvamos dados em formato **Parquet** particionado por data de registro (`ano=YYYY/mes=MM/`), otimizando custos de leitura.

### B. PostgreSQL / AWS RDS (SQLAlchemy)
Banco de dados relacional usado como Data Warehouse. O SQLAlchemy gerencia conexões e pool de conexões (evitando conexões órfãs ou lentidão).
Usamos a estratégia de **Upsert** (Insert or Update): se o registro já existe, atualiza; se não, insere. Evita duplicações no carregamento incremental.

---

## 8. Orquestração de Workflows (Apache Airflow)

Um pipeline em produção não é rodado manualmente. Usamos o **Apache Airflow** para agendar e monitorar a execução.
*   **DAG (Directed Acyclic Graph):** A definição do fluxo de tarefas e suas dependências.
*   **Task Group:** Agrupamento lógico de tarefas (ex: extrair dados de 3 fontes em paralelo antes de passar para a limpeza).
*   **SLA (Service Level Agreement):** Alerta automático se o pipeline demorar mais do que o esperado para executar.

---

## 9. Desafios Práticos de Fixação (Mão na Massa!)

Para testar o seu aprendizado na prática, abra o terminal na pasta do projeto e tente realizar as seguintes atividades no código:

1.  **Desafio 1 (Limpeza):** No arquivo [transformers.py](file:///C:/Users/tesou/.gemini/antigravity/scratch/ecommerce-etl-pipeline/src/transform/transformers.py), crie uma função simples que receba o DataFrame de clientes e formate o CPF para o padrão `999.999.999-99` (removendo pontos e traços antes, para unificar, e inserindo a máscara padrão).
2.  **Desafio 2 (Qualidade):** No arquivo [data_quality_checker.py](file:///C:/Users/tesou/.gemini/antigravity/scratch/ecommerce-etl-pipeline/src/quality/data_quality_checker.py), adicione uma nova regra que valide se a data do pedido (`order_date`) não é anterior ao ano de fundação da empresa (ex: 2020-01-01).
3.  **Desafio 3 (Query SQL):** Abra o **SQL Sandbox** no simulador do seu navegador e tente escrever uma query para descobrir qual o método de pagamento (`payment_method`) que gerou a maior média de vendas.
