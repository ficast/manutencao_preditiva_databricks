# Lakehouse para Manufatura e Manutenção Preditiva (Databricks)

Este repositório demonstra, de ponta a ponta, a construção de um Lakehouse no Databricks para manufatura e manutenção preditiva, utilizando o padrão Bronze → Silver → Gold com Delta Lake. O foco é reduzir downtime e otimizar OEE (Overall Equipment Effectiveness) através da integração de dados de IoT, ERP e Qualidade.

As instruções e notebooks foram pensadas para rodar também no Databricks Free Edition (Community/Trial), com observações sobre ambientes com/sem Unity Catalog.

## Arquitetura e Fluxo

**Bronze** → **Silver** → **Gold**

- **Bronze**: Dados brutos heterogêneos (IoT, ERP, Qualidade) gerados localmente no PostgreSQL e ingeridos para Delta Lake
- **Silver**: Curadoria incremental com `MERGE`, watermark, deduplicação e SCD Type 2 para equipamentos
- **Gold**: Modelo dimensional otimizado para análise de OEE, downtime e manutenção preditiva

## Estrutura do repositório

- **scripts/**
  - `generators/`: Scripts Python para gerar dados sintéticos no PostgreSQL local
  - `ingestion/`: Script de ingestão do PostgreSQL para Databricks Bronze

- **notebooks/**
  - `bronze_layer.ipynb`: Cria schema e tabelas Bronze no Databricks
  - `silver_layer.ipynb`: Notebook de desenvolvimento/teste para processar Bronze → Silver
  - `gold_layer.ipynb`: Notebook de desenvolvimento/teste para criar modelo Gold

- **models/**
  - `silver/*.dbquery.ipynb` e `gold/*.dbquery.ipynb`: Queries SQL individuais extraídas dos notebooks, prontas para orquestração nos jobs (cada arquivo = uma query do job)

- **jobs/**
  - `manufacturing_lakehouse_job.yaml`: Job Databricks Asset Bundle para orquestração

## Objetos criados (tabelas e views)

### Bronze (schema: `bronze`)
- `equipment_master`: cadastro de equipamentos
- `iot_sensor_readings`: leituras de sensores IoT
- `production_orders`: ordens de produção do ERP
- `maintenance_orders`: ordens de manutenção do ERP
- `quality_inspections`: inspeções de qualidade

### Silver (schema: `silver`)
- `equipment_clean`: equipamentos normalizados (dedupe + hash idempotente)
- `equipment_scd`: SCD Type 2 para equipamentos (`effective_start`, `effective_end`, `is_current`)
- `iot_readings_clean`: leituras IoT normalizadas (dedupe por `equipment_id`, `sensor_id`, `reading_timestamp`)
- `production_orders_clean`: ordens de produção curadas (dedupe + hash)
- `maintenance_orders_clean`: ordens de manutenção curadas (dedupe + hash)
- `quality_inspections_clean`: inspeções curadas (dedupe + hash)

### Gold (schema: `gold`)
- `dim_tempo`: dimensão de tempo (insert-only com `tempo_sk`)
- `dim_equipamento`: snapshot corrente de equipamentos (NK=`equipment_id`, SK gerado)
- `dim_equipamento_scd`: versão time-aware completa da SCD da Silver
- `dim_produto`: dimensão de produtos fabricados
- `dim_tecnico`: dimensão de técnicos de manutenção
- `dim_tipo_manutencao`: dimensão de tipos de manutenção
- `dim_defeito`: dimensão de códigos de defeito
- `fact_producao`: fato de produção (grão: ordem + equipamento, particionada por tempo)
- `fact_manutencao`: fato de manutenção (grão: ordem de manutenção, particionada por tempo)
- `fact_qualidade`: fato de qualidade (grão: inspeção, particionada por tempo)
- `fact_iot_agregado`: fato agregado de leituras IoT por hora (particionada por tempo)
- Views: `vw_oee_diario`, `vw_downtime_por_causa`, `vw_equipamentos_criticos`, `vw_tendencias_sensores`

## Como executar

### Geração de Dados Locais (PostgreSQL)

Este projeto inclui scripts Python para gerar dados sintéticos localmente antes de ingerir para o Databricks.

**Pré-requisitos:**
1. Inicie o PostgreSQL: `docker-compose up -d`
2. Instale dependências: `uv sync`
3. Configure `.env` com credenciais do Databricks (para ingestão)

**Sequência de geração (ordem obrigatória):**
```bash
# 1. Equipment Master (base - obrigatório primeiro)
uv run equipment-gen --mode insert --count 50

# 2. Production Orders
uv run production-gen --mode insert --count 30

# 3. Maintenance Orders
uv run maintenance-gen --mode insert --count 20

# 4. IoT Sensor Readings (pode rodar a qualquer momento após passo 1)
uv run iot-gen --count 1000 --hours-back 24

# 5. Quality Inspections (requer Production Orders)
uv run quality-gen --mode insert --count 25
```

**Verificar dados:**
```bash
uv run test-db
```

**Modos disponíveis:**
- `--mode insert`: Novos registros
- `--mode update`: Atualiza existentes
- `--mode upsert`: Mistura novos e atualizações

**Ingestão para Databricks:**

**Pré-requisito:** Crie as tabelas no Databricks antes de executar a ingestão. Execute o script SQL em `scripts/databricks/create_bronze_tables.sql` no Databricks SQL Editor.

```bash
# Primeira carga (detecta automaticamente)
uv run ingest --table equipment_master

# Todas as tabelas
uv run ingest --table all

# Atualizações incrementais (últimos 7 dias)
uv run ingest --table all --days-back 7

# Usar método Spark (opcional)
uv run ingest --table all --method spark
```

### Processamento no Databricks

1) **Importe os notebooks** para o seu workspace Databricks (via Repos ou upload manual).

2) **Crie um cluster** com suporte a Delta Lake.

3) **Configure Unity Catalog** (opcional):
- Com Unity Catalog: use catálogo `manufatura_lakehouse` e schemas `bronze`, `silver`, `gold`
- Sem Unity Catalog: defina `CATALOG = None` nos notebooks e use apenas databases

4) **Ordem de execução:**
- `notebooks/bronze_layer.ipynb` (cria schema e tabelas Bronze - execute uma vez)
- Execute a ingestão local: `uv run ingest --table all`
- `notebooks/silver_layer.ipynb` (processa dados da Bronze)
- `notebooks/gold_layer.ipynb` (cria modelo dimensional)

5) **Validações:**
```sql
SELECT * FROM gold.vw_oee_diario ORDER BY data, equipment_name;
SELECT * FROM gold.vw_downtime_por_causa ORDER BY downtime_minutes DESC;
SELECT * FROM gold.vw_equipamentos_criticos LIMIT 10;
```

## Incremental e Idempotência

- Silver usa `MERGE INTO` com hashes de linha para evitar updates sem mudanças reais.
- Deduplicações determinísticas por janela (`ROW_NUMBER()`) em todas as tabelas.
- Watermarks (ex.: 60–90 dias) controlam janelas incrementais.
- Gold reusa hashes e chaves de negócio para `MERGE` idempotente.

## SCD Type 2 (Equipamentos)

- Implementado em `silver.equipment_scd` com colunas `effective_start`, `effective_end`, `is_current` e `row_hash`.
- Publicado em Gold como:
  - `gold.dim_equipamento` (snapshot corrente; 1 linha por `nk_equipment_id`).
  - `gold.dim_equipamento_scd` (histórico completo para joins time-aware).

## Métricas de OEE

OEE (Overall Equipment Effectiveness) é calculado através de três componentes:

- **Disponibilidade** = (Tempo de Operação) / (Tempo Planejado)
  - Baseado em `fact_producao` e `fact_manutencao`
  
- **Performance** = (Quantidade Real) / (Quantidade Ideal no Tempo de Operação)
  - Baseado em `fact_producao` (actual_quantity vs planned_quantity ajustado pelo tempo)
  
- **Qualidade** = (Quantidade Boa) / (Quantidade Total)
  - Baseado em `fact_qualidade` (passed_quantity vs total_quantity)

- **OEE Total** = Disponibilidade × Performance × Qualidade

A view `vw_oee_diario` calcula essas métricas agregadas por dia e equipamento.

## Job (Databricks Asset Bundle)

- Arquivo: `jobs/manufacturing_lakehouse_job.yaml`.
- Define o job "manufacturing lakehouse" com tarefas e dependências em SQL (Databricks SQL Warehouse requerido):
  - Silver: `equipment_clean`, `equipment_scd`, `iot_readings_clean`, `production_orders_clean`, `maintenance_orders_clean`, `quality_inspections_clean`.
  - Gold: `dim_tempo`, `dim_equipamento`, `dim_equipamento_scd`, `dim_produto`, `dim_tecnico`, `dim_tipo_manutencao`, `dim_defeito`, `fact_producao`, `fact_manutencao`, `fact_qualidade`, `fact_iot_agregado`.
- Observações importantes:
  - Os campos `query_id` e `warehouse_id` são específicos do seu workspace. Você precisará criar as queries no editor SQL (copiando dos notebooks) e substituir os IDs no YAML, ou orquestrar via notebooks em um cluster all-purpose.
  - Em workspaces sem Unity Catalog ou sem SQL Warehouse, execute via notebooks na ordem indicada acima.

## Troubleshooting

**PostgreSQL:**
- **Collation version mismatch**: Execute `ALTER DATABASE manufatura_raw REFRESH COLLATION VERSION;` (é apenas um aviso, não impede funcionamento)
- **Tabelas não aparecem no DBeaver**: Verifique se está conectado no banco `manufatura_raw` e schema `bronze`

**Databricks:**
- **Erro em `USE CATALOG`**: Workspace sem Unity Catalog - defina `CATALOG = None` nos notebooks
- **Permissões**: Garanta permissões para criar schemas `bronze`, `silver`, `gold`
- **Ingestão falha**: Verifique credenciais no `.env` (DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_ACCESS_TOKEN)

## Licença

Uso educacional/workshop. Ajuste conforme sua necessidade organizacional.


