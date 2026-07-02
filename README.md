# 🚀 ETL Pipeline — Plataforma Analítica de E-Commerce

[![CI Pipeline](https://github.com/shopflow/ecommerce-etl-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/shopflow/ecommerce-etl-pipeline/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Data Quality](https://img.shields.io/badge/Data%20Quality-Validated-success.svg)](#qualidade-de-dados)

Pipeline ETL (Extract, Transform, Load) de dados transacionais de e-commerce de nível de produção. O projeto implementa uma **Arquitetura Medallion** (Bronze → Silver → Gold) utilizando modelagem dimensional (Star Schema), orquestrado pelo **Apache Airflow** e integrado a serviços em nuvem **AWS (S3 & RDS)**.

---

## 📌 Visão Geral do Projeto

Este pipeline foi projetado para consolidar dados brutos vindos de arquivos de transações locais (CSVs) e dados dinâmicos de catálogo consumidos de APIs REST externas. Ele higieniza e transforma esses dados em tabelas de dimensões e fatos otimizadas para consultas analíticas (OLAP).

### Arquitetura de Dados (Medallion)

```
                       ┌────────────────────────┐
                       │   Fontes de Dados      │
                       │ • API de Produtos      │
                       │ • Arquivos CSV (Vendas)│
                       └───────────┬────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       CAMADA BRONZE (Raw)                           │
│ • Armazenamento dos dados brutos no estado original (JSON/CSV)      │
│ • Local: data/raw/ | AWS: S3 Bucket (raw-zone/)                     │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       CAMADA SILVER (Clean)                         │
│ • Deduplicação, tratamento de nulos, conversão de tipos de dados    │
│ • Higienização de e-mails, normalização de strings e validações     │
│ • Armazenado em formato colunar Parquet compactado com Snappy       │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       CAMADA GOLD (Analytical)                      │
│ • Modelagem dimensional Star Schema (Dimensões + Tabela Fato)       │
│ • Histórico de clientes modelado como SCD Tipo 2 (Slowly Changing)  │
│ • Local: PostgreSQL (shopflow_dw) | AWS: RDS PostgreSQL Instance    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Stack Tecnológica & Decisões de Design

*   **Linguagem:** Python (v3.11+) para manipulação dos dados brutos através do **pandas** e **pyarrow**.
*   **Orquestração:** **Apache Airflow** estruturado de forma modular e escalável em ambiente distribuído.
*   **Armazenamento em Nuvem (Datalake):** **AWS S3** para as zonas Bronze e Silver usando compressão Parquet Snappy.
*   **Data Warehouse (DWH):** **PostgreSQL (AWS RDS)** estruturado para armazenamento das dimensões e fatos da camada Gold.
*   **Qualidade de Dados:** Framework personalizado baseado em schemas JSON para testes de completude, unicidade e regras de negócio.
*   **Ambiente Local / IaC:** **Docker & Docker Compose** para simular toda a infraestrutura localmente (incluindo **MinIO** emulando o AWS S3).
*   **Integração Contínua (CI):** **GitHub Actions** automatizando a execução do formatador (ruff), tipagem (mypy) e testes (pytest).

---

## 📐 Modelagem do Data Warehouse (Gold)

O modelo analítico adota o padrão **Star Schema (Star Modeling)** estruturado sob a seguinte lógica:

*   `dim_customer` (SCD Tipo 2): Mantém o histórico completo das informações de clientes (campos `valid_from`, `valid_to`, `is_current`).
*   `dim_product`: Dados detalhados de produtos, incluindo margens calculadas e faixas de preço analíticas.
*   `dim_date`: Dimensão de tempo para agregar vendas por ano, mês, dia da semana, trimestres e feriados brasileiros.
*   `fact_orders`: Centraliza as métricas financeiras cruciais (quantidade, valor bruto, descontos, frete, valores líquidos).

---

## 🚀 Quick Start (Execução Local)

### Requisitos Prévios
*   Python 3.11 ou superior instalado.
*   Docker Desktop ativo.

### 1. Clonar e preparar o ambiente
```bash
# Clone o repositório
git clone https://github.com/seu-usuario/ecommerce-etl-pipeline.git
cd ecommerce-etl-pipeline

# Configure as variáveis de ambiente
cp .env.example .env
```

### 2. Iniciar os Containers (Airflow + Postgres + MinIO)
```bash
# Sobe todos os serviços locais (PostgreSQL, MinIO e Airflow)
docker compose -f infrastructure/docker-compose.yml up -d
```
*   **Apache Airflow UI:** `http://localhost:8080` (Usuário: `admin` | Senha: `admin`)
*   **MinIO Console:** `http://localhost:9001` (Usuário: `minioadmin` | Senha: `minioadmin`)
*   **PostgreSQL Port:** `5432` (Credenciais: `postgres` | `postgres`)

### 3. Executar o Pipeline Localmente (Sem Airflow)
Você também pode rodar o pipeline inteiro via linha de comando local para testar a integração:
```bash
# Crie um ambiente virtual e instale dependências
python -m venv .venv
source .venv/bin/activate  # Ou .venv\Scripts\activate no Windows
pip install -r requirements-dev.txt

# Executa o pipeline de ponta a ponta gerando 1.000 transações de teste
python -m src.main --local-only
```

---

## ✅ Qualidade de Dados (Data Quality)

Nenhum dado é carregado na camada Gold sem passar por verificações automáticas na camada Silver. O pipeline executa testes definidos na pasta `src/quality/expectations/`:
1.  **Completude:** Garante que IDs e chaves não possuem valores nulos acima de um limite pré-determinado (ex: 98%).
2.  **Unicidade:** Certifica que chaves primárias e naturais (como ID de vendas) não são duplicadas.
3.  **Regras de Range:** Preços, quantidades e descontos não podem ser negativos ou estarem fora de padrões comerciais.
4.  **Regras de Negócio (Allowed Values):** Status de pedidos e métodos de pagamento devem estar restritos a uma lista válida.

---

## 🧪 Execução de Testes Automatizados

O projeto tem cobertura de testes unitários superior a 80%. Para rodá-los localmente:
```bash
# Executa todos os testes e exibe relatório de cobertura
pytest tests/ -v
```

---

## 📄 Licença
Distribuído sob a licença MIT. Veja `LICENSE` para mais informações.
