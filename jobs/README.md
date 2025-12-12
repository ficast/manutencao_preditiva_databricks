# Databricks Job - Manufacturing Lakehouse Pipeline

Este diretório contém a definição do job Databricks para executar o pipeline completo de Silver e Gold layers.

## Arquivo

- `manufacturing_lakehouse_job.yaml`: Definição do job com todas as tasks e dependências

## Como Importar no Databricks

### Opção 1: Via UI (Databricks Workspace)

1. Acesse **Workflows** > **Jobs** no Databricks
2. Clique em **Create Job**
3. Selecione **Import from file**
4. Faça upload do arquivo `manufacturing_lakehouse_job.yaml`
5. Ajuste as configurações conforme necessário:
   - **Cluster**: Ajuste `node_type_id` e `num_workers` conforme seu ambiente
   - **Catalog/Schema**: Verifique se os parâmetros `catalog`, `schema_bronze`, `schema_silver`, `schema_gold` estão corretos
   - **Notebook paths**: Verifique se os caminhos dos notebooks estão corretos (ex: `/models/silver/equipment_clean`)

### Opção 2: Via Databricks CLI

```bash
# Instalar Databricks CLI (se ainda não tiver)
pip install databricks-cli

# Configurar autenticação
databricks configure --token

# Importar job
databricks jobs create --json-file manufacturing_lakehouse_job.yaml
```

### Opção 3: Via Databricks Asset Bundles (DAB)

Se você estiver usando Databricks Asset Bundles:

```bash
# No diretório do projeto
databricks bundle deploy
databricks bundle run
```

## Estrutura do Job

### Silver Layer (6 tasks)
1. `silver_equipment_clean` - Limpeza de equipamentos
2. `silver_equipment_scd` - SCD Type 2 (depende de equipment_clean)
3. `silver_iot_readings_clean` - Limpeza de IoT (paralelo)
4. `silver_production_orders_clean` - Limpeza de ordens de produção (paralelo)
5. `silver_maintenance_orders_clean` - Limpeza de ordens de manutenção (paralelo)
6. `silver_quality_inspections_clean` - Limpeza de inspeções (paralelo)

### Gold Layer - Dimensões (7 tasks)
1. `gold_dim_tempo` - Dimensão de tempo
2. `gold_dim_equipamento` - Snapshot de equipamentos
3. `gold_dim_equipamento_scd` - SCD Type 2 de equipamentos
4. `gold_dim_produto` - Dimensão de produtos
5. `gold_dim_tecnico` - Dimensão de técnicos
6. `gold_dim_tipo_manutencao` - Dimensão de tipos de manutenção
7. `gold_dim_defeito` - Dimensão de códigos de defeito

### Gold Layer - Fatos (4 tasks)
1. `gold_fact_producao` - Fato de produção
2. `gold_fact_manutencao` - Fato de manutenção
3. `gold_fact_qualidade` - Fato de qualidade
4. `gold_fact_iot_agregado` - Fato agregado de IoT

### Gold Layer - Views (4 tasks)
1. `gold_vw_oee_diario` - View de OEE diário
2. `gold_vw_downtime_por_causa` - View de downtime
3. `gold_vw_equipamentos_criticos` - View de equipamentos críticos
4. `gold_vw_tendencias_sensores` - View de tendências de sensores

## Dependências

O job garante que:
- Silver layer roda primeiro
- Dimensões Gold dependem das tabelas Silver correspondentes
- Fatos Gold dependem das dimensões necessárias
- Views dependem dos fatos

## Configurações Importantes

### Cluster
- **Spark Version**: `15.4.x-scala2.12` (ajuste conforme disponível)
- **Node Type**: `i3.xlarge` (ajuste conforme seu ambiente e custos)
- **Workers**: 2 (ajuste conforme volume de dados)

### Catalog e Schemas
Por padrão, o job usa:
- **Catalog**: `manufatura_lakehouse`
- **Bronze Schema**: `bronze`
- **Silver Schema**: `silver`
- **Gold Schema**: `gold`

Se você não usar Unity Catalog, ajuste os parâmetros nos notebooks para `catalog: None`.

### Notebook Paths
Os notebooks devem estar no workspace do Databricks em:
- `/models/silver/*.dbquery.ipynb`
- `/models/gold/*.dbquery.ipynb`

**Importante**: Certifique-se de que os notebooks foram importados para o workspace antes de executar o job.

## Agendamento

Para agendar o job, descomente a seção `schedule` no YAML:

```yaml
schedule:
  quartz_cron_expression: "0 0 2 * * ?"  # Diário às 2h
  timezone_id: "America/Sao_Paulo"
  pause_status: "UNPAUSED"
```

## Troubleshooting

### Erro: "Notebook not found"
- Verifique se os notebooks foram importados para o workspace
- Confirme os caminhos no YAML estão corretos
- Use caminhos absolutos começando com `/`

### Erro: "Catalog not found"
- Verifique se o catalog `manufatura_lakehouse` existe
- Se não usar Unity Catalog, ajuste os parâmetros para `catalog: None`

### Erro: "Cluster not available"
- Verifique se o `node_type_id` está disponível no seu workspace
- Ajuste para um tipo de nó disponível (ex: `standard_DS3_v2`)

### Timeout
- Aumente `timeout_seconds` no YAML se o job demorar mais de 2 horas
- Considere aumentar o tamanho do cluster para processar mais rápido

