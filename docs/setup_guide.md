# 📖 Guia de Configuração e AWS Cloud Setup

Este documento descreve detalhadamente como configurar a infraestrutura de nuvem AWS para mover o pipeline do ambiente Docker local para a nuvem de produção.

---

## ☁️ 1. Configurando Serviços AWS

Para implantar o pipeline na nuvem, você precisará configurar os seguintes serviços na sua conta AWS:

### A. AWS IAM (Identity and Access Management)
Crie um usuário com permissões de acesso programático para o pipeline:
1. Acesse o console do IAM na AWS.
2. Crie um novo grupo de usuários chamado `data-engineering-group`.
3. Anexe as seguintes políticas padrão da AWS a este grupo:
   - `AmazonS3FullAccess`
   - `AmazonRDSFullAccess`
4. Crie um usuário chamado `shopflow-etl-user` e adicione-o a esse grupo.
5. Vá para a aba **Security credentials** (Credenciais de segurança) deste usuário.
6. Clique em **Create access key** (Criar chave de acesso), escolha **Command Line Interface (CLI)** e salve as credenciais (`AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY`).

### B. AWS S3 (Simple Storage Service)
Crie o Datalake de armazenamento bruto e higienizado:
1. Vá para o painel do S3 no console AWS.
2. Clique em **Create bucket**.
3. Defina um nome globalmente exclusivo (ex: `shopflow-data-lake-seu-nome`).
4. Escolha sua região AWS de preferência (ex: `us-east-1`).
5. Mantenha as configurações padrões (bloqueio de acesso público padrão) e clique em **Create bucket**.

### C. AWS RDS (Relational Database Service)
Crie a instância de banco de dados PostgreSQL para servir de Data Warehouse (Gold):
1. Acesse o painel do RDS no console AWS.
2. Clique em **Create database**.
3. Escolha **Standard create** (Criação padrão) e selecione o motor **PostgreSQL**.
4. Em **Templates**, selecione **Free Tier** (Nível gratuito) para evitar cobranças indesejadas.
5. Configurações de identificação:
   - **DB instance identifier:** `shopflow-db-instance`
   - **Master username:** `postgres`
   - **Master password:** Crie uma senha segura (ex: `shopflowSecurePass2026`).
6. Configurações de Conectividade:
   - Defina **Public access** (Acesso público) como **Yes** (para que possamos acessar a instância a partir do nosso ambiente local para testes, em produção real isso deve ser desabilitado).
7. Clique em **Create database**. O RDS levará alguns minutos para inicializar.
8. Uma vez criado, copie o **Endpoint** do banco na aba de conectividade (ele servirá como o `RDS_HOST`).

---

## ⚙️ 2. Atualizando Variáveis de Ambiente (.env)

Após criar a infraestrutura AWS, edite o arquivo `.env` do projeto local substituindo as chaves temporárias do MinIO pelas reais da nuvem AWS:

```env
# AWS Configurations (Substitua pelos dados reais da nuvem)
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_REGION=us-east-1
S3_BUCKET_NAME=shopflow-data-lake-seu-nome

# RDS / PostgreSQL Configurations (AWS RDS Endpoint)
RDS_HOST=shopflow-db-instance.xxxxxx.us-east-1.rds.amazonaws.com
RDS_PORT=5432
RDS_DATABASE=shopflow_dw
RDS_USER=postgres
RDS_PASSWORD=shopflowSecurePass2026

# Environment settings
ENVIRONMENT=production
LOG_LEVEL=INFO
```

---

## 🐳 3. Executando o Pipeline no Docker apontando para AWS

Quando você mudar a variável `ENVIRONMENT` para `production` (ou qualquer valor diferente de `development` ou `testing`), as classes `S3Loader` e `RDSLoader` ignoram os endpoints locais (localhost/MinIO) e passam a apontar diretamente para os servidores da AWS usando a autenticação fornecida por `AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY`.

Para rodar o Airflow orquestrando em direção à AWS:
1. Certifique-se de atualizar o docker-compose ou o `.env` montado.
2. Reinicie as instâncias do Airflow:
   ```bash
   docker compose -f infrastructure/docker-compose.yml down
   docker compose -f infrastructure/docker-compose.yml up -d
   ```
3. Acesse a interface web do Airflow em `http://localhost:8080` e execute a DAG `ecommerce_etl_pipeline`. Os dados agora serão salvos no seu Bucket S3 real e carregados no banco de dados RDS PostgreSQL!
