# Guia Passo a Passo: Criar Dashboard no Databricks SQL

## Passo 1: Acessar SQL Dashboards
1. No Databricks, clique em **SQL** no menu lateral
2. Clique em **Dashboards** no menu superior
3. Clique em **Create Dashboard**

## Passo 2: Configurar Dashboard
- **Nome**: `Manufatura - OEE e Performance`
- Clique em **Create**

## Passo 3: Adicionar KPIs (4 Cards)

### Card 1: OEE Médio
- Tipo: **Text/Single Value**
- Query:
SELECT 
    ROUND(AVG(availability_rate * performance_rate * quality_rate) * 100, 2) as oee_medio_pct
FROM manufatura_lakehouse.gold.vw_oee_diario
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 30);

- Config: Title = "OEE Médio (30d)", Value = `oee_medio_pct`, Suffix = "%"

### Card 2: Downtime Hoje
- Tipo: **Text/Single Value**
- Query:
SELECT 
    COALESCE(SUM(total_downtime_minutes), 0) as downtime_hoje_min
FROM manufatura_lakehouse.gold.vw_downtime_por_causa
WHERE full_date = CURRENT_DATE();- Config: Title = "Downtime Hoje", Value = `downtime_hoje_min`, Suffix = " min"

### Card 3: Equipamentos Críticos
- Tipo: **Text/Single Value**
- Query:
SELECT 
    COUNT(*) as equipamentos_criticos
FROM manufatura_lakehouse.gold.vw_equipamentos_criticos
WHERE criticality_score > 100;- Config: Title = "Equipamentos Críticos", Value = `equipamentos_criticos`

### Card 4: Taxa de Qualidade
- Tipo: **Text/Single Value**
- Query:
SELECT 
    ROUND(AVG(quality_rate) * 100, 2) as taxa_qualidade_pct
FROM manufatura_lakehouse.gold.vw_oee_diario
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 30);- Config: Title = "Taxa de Qualidade (30d)", Value = `taxa_qualidade_pct`, Suffix = "%"

## Passo 4: OEE por Equipamento (Bar Chart)
- Tipo: **Bar Chart**
- Query:
SELECT 
    equipment_name,
    ROUND(AVG(availability_rate * performance_rate * quality_rate) * 100, 2) as oee_pct
FROM manufatura_lakehouse.gold.vw_oee_diario
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY equipment_name
ORDER BY oee_pct DESC;- Config: X = `equipment_name`, Y = `oee_pct`, Title = "OEE por Equipamento (7 dias)"

## Passo 5: Evolução OEE (Line Chart)
- Tipo: **Line Chart**
- Query:
SELECT 
    full_date,
    equipment_name,
    ROUND((availability_rate * performance_rate * quality_rate) * 100, 2) as oee_pct
FROM manufatura_lakehouse.gold.vw_oee_diario
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY full_date, equipment_name;- Config: X = `full_date`, Y = `oee_pct`, Series = `equipment_name`, Title = "Evolução OEE (30 dias)"

## Passo 6: Top 10 Equipamentos Críticos (Table)
- Tipo: **Table**
- Query:
SELECT 
    equipment_name,
    total_downtime,
    maintenance_count,
    total_failures,
    criticality_score
FROM manufatura_lakehouse.gold.vw_equipamentos_criticos
ORDER BY criticality_score DESC
LIMIT 10;- Config: Title = "Top 10 Equipamentos Críticos"

## Passo 7: Downtime por Causa (Pie Chart)
- Tipo: **Pie Chart**
- Query:
SELECT 
    COALESCE(maintenance_type, 'Não especificado') as causa,
    SUM(total_downtime_minutes) as total_minutos
FROM manufatura_lakehouse.gold.vw_downtime_por_causa
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY maintenance_type
ORDER BY total_minutos DESC;- Config: Label = `causa`, Value = `total_minutos`, Title = "Downtime por Causa (30 dias)"

## Passo 8: Downtime por Equipamento (Bar Chart)
- Tipo: **Bar Chart**
- Query:l
SELECT 
    equipment_name,
    SUM(total_downtime_minutes) as total_downtime_min
FROM manufatura_lakehouse.gold.vw_downtime_por_causa
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY equipment_name
ORDER BY total_downtime_min DESC
LIMIT 10;- Config: X = `equipment_name`, Y = `total_downtime_min`, Title = "Top 10 Downtime por Equipamento"

## Passo 9: Tendências Sensores (Line Chart)
- Tipo: **Line Chart**
- Query:
SELECT 
    full_date,
    equipment_name,
    sensor_type,
    avg_reading
FROM manufatura_lakehouse.gold.vw_tendencias_sensores
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 7)
ORDER BY full_date, equipment_name, sensor_type;- Config: X = `full_date`, Y = `avg_reading`, Series = `sensor_type`, Title = "Tendências Sensores (7 dias)"

## Passo 10: Tabela OEE Detalhada
- Tipo: **Table**
- Query:
SELECT 
    full_date,
    equipment_name,
    downtime_min,
    production_minutes,
    ROUND(availability_rate * 100, 2) as disponibilidade_pct,
    ROUND(performance_rate * 100, 2) as performance_pct,
    ROUND(quality_rate * 100, 2) as qualidade_pct,
    ROUND((availability_rate * performance_rate * quality_rate) * 100, 2) as oee_pct
FROM manufatura_lakehouse.gold.vw_oee_diario
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 7)
ORDER BY full_date DESC, oee_pct ASC;- Config: Title = "OEE Detalhado (7 dias)", Enable Filtering = Sim

## Organização do Layout
- Linha 1: 4 KPIs lado a lado
- Linha 2: OEE por Equipamento (largura total)
- Linha 3: Evolução OEE (largura total)
- Linha 4: Top 10 Críticos (esquerda) | Downtime por Causa (direita)
- Linha 5: Downtime por Equipamento (esquerda) | Tendências Sensores (direita)
- Linha 6: Tabela OEE Detalhada (largura total)