-- ============================================================
-- Script para criar schema e tabelas Bronze no Databricks
-- Execute este script no Databricks SQL Editor ou Notebook
-- ============================================================

-- Configurar catalog e schema (ajuste conforme seu ambiente)
-- CATALOG padrão: main (ou o catalog configurado no .env)
-- SCHEMA: bronze

-- Criar schema bronze (se não existir)
CREATE SCHEMA IF NOT EXISTS bronze;

-- ============================================================
-- equipment_master
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.equipment_master (
    equipment_id      STRING,
    equipment_name    STRING,
    equipment_type    STRING,
    location          STRING,
    installation_date STRING,
    manufacturer      STRING,
    model             STRING,
    status            STRING,
    last_update_date  STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================
-- iot_sensor_readings
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.iot_sensor_readings (
    reading_id        STRING,
    equipment_id       STRING,
    sensor_id          STRING,
    sensor_type        STRING,
    reading_value      STRING,
    reading_timestamp  STRING,
    unit               STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================
-- production_orders
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.production_orders (
    production_order_id STRING,
    equipment_id        STRING,
    product_id          STRING,
    planned_start       STRING,
    planned_end         STRING,
    actual_start        STRING,
    actual_end          STRING,
    planned_quantity    STRING,
    actual_quantity     STRING,
    status              STRING,
    last_update         STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================
-- maintenance_orders
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.maintenance_orders (
    maintenance_order_id STRING,
    equipment_id         STRING,
    maintenance_type     STRING,
    scheduled_start      STRING,
    scheduled_end        STRING,
    actual_start         STRING,
    actual_end           STRING,
    technician_id        STRING,
    status               STRING,
    priority             STRING,
    description          STRING,
    last_update          STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================
-- quality_inspections
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.quality_inspections (
    inspection_id       STRING,
    production_order_id STRING,
    equipment_id        STRING,
    inspection_type     STRING,
    inspection_date     STRING,
    inspector_id        STRING,
    passed              STRING,
    failed_quantity     STRING,
    total_quantity      STRING,
    defect_codes        STRING,
    notes               STRING,
    last_update         STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================
-- Comentários nas tabelas
-- ============================================================
COMMENT ON SCHEMA bronze IS 'Schema Bronze - dados brutos simulando fonte de dados heterogênea';
COMMENT ON TABLE bronze.equipment_master IS 'Cadastro de equipamentos - suporta INSERT e UPDATE';
COMMENT ON TABLE bronze.iot_sensor_readings IS 'Leituras de sensores IoT - apenas INSERT (append-only)';
COMMENT ON TABLE bronze.production_orders IS 'Ordens de produção - suporta INSERT e UPDATE';
COMMENT ON TABLE bronze.maintenance_orders IS 'Ordens de manutenção - suporta INSERT e UPDATE';
COMMENT ON TABLE bronze.quality_inspections IS 'Inspeções de qualidade - suporta INSERT e UPDATE';


