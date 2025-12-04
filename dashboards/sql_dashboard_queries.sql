-- ============================================================
-- Queries SQL para Databricks SQL Dashboards
-- ============================================================
-- Use estas queries no Databricks SQL > Dashboards
-- Catalog: manufatura_lakehouse
-- Schema: gold
-- ============================================================

-- ============================================================
-- 1. KPIs Principais (Cards)
-- ============================================================

-- OEE Médio (últimos 30 dias)
SELECT 
    ROUND(AVG(availability_rate * performance_rate * quality_rate) * 100, 2) as oee_medio_pct
FROM manufatura_lakehouse.gold.vw_oee_diario
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 30);

-- Total Downtime Hoje (minutos)
SELECT 
    COALESCE(SUM(total_downtime_minutes), 0) as downtime_hoje_min
FROM manufatura_lakehouse.gold.vw_downtime_por_causa
WHERE full_date = CURRENT_DATE();

-- Equipamentos Críticos (count)
SELECT 
    COUNT(*) as equipamentos_criticos
FROM manufatura_lakehouse.gold.vw_equipamentos_criticos
WHERE criticality_score > 100;

-- Taxa de Qualidade Média (últimos 30 dias)
SELECT 
    ROUND(AVG(quality_rate) * 100, 2) as taxa_qualidade_pct
FROM manufatura_lakehouse.gold.vw_oee_diario
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 30);

-- ============================================================
-- 2. OEE por Equipamento (Bar Chart)
-- ============================================================
SELECT 
    equipment_name,
    ROUND(AVG(availability_rate * performance_rate * quality_rate) * 100, 2) as oee_pct
FROM manufatura_lakehouse.gold.vw_oee_diario
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY equipment_name
ORDER BY oee_pct DESC;

-- ============================================================
-- 3. Evolução OEE (Line Chart)
-- ============================================================
SELECT 
    full_date,
    equipment_name,
    ROUND((availability_rate * performance_rate * quality_rate) * 100, 2) as oee_pct
FROM manufatura_lakehouse.gold.vw_oee_diario
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY full_date, equipment_name;

-- ============================================================
-- 4. Top 10 Equipamentos Críticos (Table)
-- ============================================================
SELECT 
    equipment_name,
    total_downtime,
    maintenance_count,
    total_failures,
    criticality_score
FROM manufatura_lakehouse.gold.vw_equipamentos_criticos
ORDER BY criticality_score DESC
LIMIT 10;

-- ============================================================
-- 5. Downtime por Causa (Pie Chart)
-- ============================================================
SELECT 
    COALESCE(maintenance_type, 'Não especificado') as causa,
    SUM(total_downtime_minutes) as total_minutos
FROM manufatura_lakehouse.gold.vw_downtime_por_causa
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY maintenance_type
ORDER BY total_minutos DESC;

-- ============================================================
-- 6. Downtime por Equipamento (Bar Chart)
-- ============================================================
SELECT 
    equipment_name,
    SUM(total_downtime_minutes) as total_downtime_min
FROM manufatura_lakehouse.gold.vw_downtime_por_causa
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY equipment_name
ORDER BY total_downtime_min DESC;

-- ============================================================
-- 7. Tendência de Downtime (Line Chart)
-- ============================================================
SELECT 
    full_date,
    COALESCE(maintenance_type, 'Não especificado') as causa,
    SUM(total_downtime_minutes) as total_minutos
FROM manufatura_lakehouse.gold.vw_downtime_por_causa
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY full_date, maintenance_type
ORDER BY full_date, causa;

-- ============================================================
-- 8. Tendências de Sensores (Line Chart)
-- ============================================================
SELECT 
    full_date,
    equipment_name,
    sensor_type,
    avg_reading,
    min_reading,
    max_reading
FROM manufatura_lakehouse.gold.vw_tendencias_sensores
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 7)
ORDER BY full_date, equipment_name, sensor_type;

-- ============================================================
-- 9. Range de Leituras de Sensores (Bar Chart)
-- ============================================================
SELECT 
    sensor_type,
    AVG(avg_reading) as media,
    MIN(min_reading) as minimo,
    MAX(max_reading) as maximo
FROM manufatura_lakehouse.gold.vw_tendencias_sensores
WHERE full_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY sensor_type
ORDER BY sensor_type;

-- ============================================================
-- 10. OEE Detalhado (Table com Filtros)
-- ============================================================
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
ORDER BY full_date DESC, oee_pct ASC;

