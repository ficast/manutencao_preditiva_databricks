# Dashboards para Camada Gold

Este diretório contém exemplos e guias para criar dashboards das views analíticas da camada Gold.

## Views Disponíveis

1. **`vw_oee_diario`**: OEE (Overall Equipment Effectiveness) diário por equipamento
2. **`vw_downtime_por_causa`**: Downtime agregado por tipo de manutenção
3. **`vw_equipamentos_criticos`**: Equipamentos com maior criticidade
4. **`vw_tendencias_sensores`**: Tendências de leituras de sensores IoT

## Opções de Dashboard

### 1. Databricks SQL Dashboards (Recomendado - Mais Simples)

**Vantagens:**
- Nativo do Databricks, sem instalação adicional
- Conecta diretamente às views Gold
- Compartilhamento fácil com a equipe
- Suporta filtros e parâmetros

**Como criar:**
1. Acesse **SQL** > **Dashboards** no Databricks
2. Clique em **Create Dashboard**
3. Adicione queries SQL para cada visualização
4. Configure gráficos (bar, line, pie, table, etc.)

**Queries de exemplo:**
Veja `sql_dashboard_queries.sql` para queries prontas.

### 2. Databricks Lakeview Dashboards (Mais Moderno)

**Vantagens:**
- Interface mais visual e intuitiva
- Melhor para usuários não-técnicos
- Suporta interatividade avançada

**Como criar:**
1. Acesse **SQL** > **Lakeview** no Databricks
2. Crie um novo dashboard
3. Arraste e solte visualizações
4. Conecte às views Gold

### 3. Streamlit App (Mais Customizável)

**Vantagens:**
- Controle total sobre visualizações
- Pode rodar como app no Databricks
- Suporta interatividade complexa
- Python nativo

**Como usar:**
1. Veja o exemplo em `streamlit_dashboard.py`
2. Execute no Databricks como notebook ou app
3. Customize conforme necessário

### 4. Power BI / Tableau (Enterprise)

**Vantagens:**
- Recursos avançados de visualização
- Melhor para apresentações executivas
- Suporta drill-down complexo

**Como conectar:**
- Use JDBC/ODBC do Databricks SQL Warehouse
- Connection string: `jdbc:databricks://<server-hostname>:443/default;transportMode=http;ssl=1;httpPath=<http-path>`
- Autenticação via Personal Access Token

## Estrutura de Dashboards Recomendada

### Dashboard Principal: OEE e Performance

**Seções:**
1. **KPIs Principais** (cards)
   - OEE Médio (últimos 30 dias)
   - Total Downtime (hoje)
   - Equipamentos Críticos (count)
   - Taxa de Qualidade (média)

2. **OEE por Equipamento** (bar chart)
   - Query: `vw_oee_diario`
   - Agrupar por: `equipment_name`
   - Filtrar: últimos 7 dias

3. **Evolução OEE** (line chart)
   - Query: `vw_oee_diario`
   - Eixo X: `full_date`
   - Eixo Y: `availability_rate * performance_rate * quality_rate`
   - Série: por equipamento

4. **Top 10 Equipamentos Críticos** (table)
   - Query: `vw_equipamentos_criticos`
   - Ordenar por: `criticality_score DESC`
   - Limitar: 10

### Dashboard: Downtime e Manutenção

**Seções:**
1. **Downtime por Causa** (pie chart)
   - Query: `vw_downtime_por_causa`
   - Agrupar por: `maintenance_type`
   - Métrica: `total_downtime_minutes`

2. **Downtime por Equipamento** (bar chart)
   - Query: `vw_downtime_por_causa`
   - Agrupar por: `equipment_name`
   - Filtrar: últimos 30 dias

3. **Tendência de Downtime** (line chart)
   - Query: `vw_downtime_por_causa`
   - Eixo X: `full_date`
   - Eixo Y: `total_downtime_minutes`
   - Agrupar por: `maintenance_type`

### Dashboard: IoT e Sensores

**Seções:**
1. **Tendências de Sensores** (line chart)
   - Query: `vw_tendencias_sensores`
   - Eixo X: `full_date`
   - Eixo Y: `avg_reading`
   - Série: `sensor_type`
   - Filtrar por: `equipment_name`

2. **Range de Leituras** (bar chart)
   - Query: `vw_tendencias_sensores`
   - Mostrar: `min_reading`, `max_reading`, `avg_reading`
   - Agrupar por: `sensor_type`

## Próximos Passos

1. **Escolha uma opção** acima
2. **Veja os exemplos** nos arquivos deste diretório
3. **Customize** conforme suas necessidades
4. **Compartilhe** com stakeholders

