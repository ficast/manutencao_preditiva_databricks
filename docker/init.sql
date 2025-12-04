-- ============================================================
-- Script de inicialização do PostgreSQL
-- Cria schema e tabelas Bronze (estrutura igual ao Databricks)
-- ============================================================

-- Criar schema bronze
CREATE SCHEMA IF NOT EXISTS bronze;

-- ============================================================
-- equipment_master
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.equipment_master (
    equipment_id      VARCHAR(255),
    equipment_name    VARCHAR(255),
    equipment_type    VARCHAR(255),
    location          VARCHAR(255),
    installation_date VARCHAR(255),
    manufacturer      VARCHAR(255),
    model             VARCHAR(255),
    status            VARCHAR(255),
    last_update_date  VARCHAR(255),
    PRIMARY KEY (equipment_id)
);

CREATE INDEX IF NOT EXISTS idx_equipment_master_last_update ON bronze.equipment_master(last_update_date);

-- ============================================================
-- iot_sensor_readings
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.iot_sensor_readings (
    reading_id        VARCHAR(255),
    equipment_id      VARCHAR(255),
    sensor_id         VARCHAR(255),
    sensor_type       VARCHAR(255),
    reading_value     VARCHAR(255),
    reading_timestamp VARCHAR(255),
    unit              VARCHAR(255),
    PRIMARY KEY (reading_id)
);

CREATE INDEX IF NOT EXISTS idx_iot_equipment_timestamp ON bronze.iot_sensor_readings(equipment_id, reading_timestamp);
CREATE INDEX IF NOT EXISTS idx_iot_timestamp ON bronze.iot_sensor_readings(reading_timestamp);

-- ============================================================
-- production_orders
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.production_orders (
    production_order_id VARCHAR(255),
    equipment_id        VARCHAR(255),
    product_id          VARCHAR(255),
    planned_start       VARCHAR(255),
    planned_end         VARCHAR(255),
    actual_start        VARCHAR(255),
    actual_end          VARCHAR(255),
    planned_quantity    VARCHAR(255),
    actual_quantity     VARCHAR(255),
    status              VARCHAR(255),
    last_update         VARCHAR(255),
    PRIMARY KEY (production_order_id)
);

CREATE INDEX IF NOT EXISTS idx_production_orders_last_update ON bronze.production_orders(last_update);
CREATE INDEX IF NOT EXISTS idx_production_orders_equipment ON bronze.production_orders(equipment_id);

-- ============================================================
-- maintenance_orders
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.maintenance_orders (
    maintenance_order_id VARCHAR(255),
    equipment_id         VARCHAR(255),
    maintenance_type     VARCHAR(255),
    scheduled_start      VARCHAR(255),
    scheduled_end        VARCHAR(255),
    actual_start         VARCHAR(255),
    actual_end           VARCHAR(255),
    technician_id        VARCHAR(255),
    status               VARCHAR(255),
    priority             VARCHAR(255),
    description          TEXT,
    last_update          VARCHAR(255),
    PRIMARY KEY (maintenance_order_id)
);

CREATE INDEX IF NOT EXISTS idx_maintenance_orders_last_update ON bronze.maintenance_orders(last_update);
CREATE INDEX IF NOT EXISTS idx_maintenance_orders_equipment ON bronze.maintenance_orders(equipment_id);

-- ============================================================
-- quality_inspections
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.quality_inspections (
    inspection_id       VARCHAR(255),
    production_order_id VARCHAR(255),
    equipment_id        VARCHAR(255),
    inspection_type     VARCHAR(255),
    inspection_date     VARCHAR(255),
    inspector_id        VARCHAR(255),
    passed              VARCHAR(255),
    failed_quantity     VARCHAR(255),
    total_quantity      VARCHAR(255),
    defect_codes        VARCHAR(255),
    notes               TEXT,
    last_update         VARCHAR(255),
    PRIMARY KEY (inspection_id)
);

CREATE INDEX IF NOT EXISTS idx_quality_inspections_last_update ON bronze.quality_inspections(last_update);
CREATE INDEX IF NOT EXISTS idx_quality_inspections_equipment ON bronze.quality_inspections(equipment_id);

-- ============================================================
-- Comentários nas tabelas
-- ============================================================
COMMENT ON SCHEMA bronze IS 'Schema Bronze - dados brutos simulando fonte de dados heterogênea';
COMMENT ON TABLE bronze.equipment_master IS 'Cadastro de equipamentos - suporta INSERT e UPDATE';
COMMENT ON TABLE bronze.iot_sensor_readings IS 'Leituras de sensores IoT - apenas INSERT (append-only)';
COMMENT ON TABLE bronze.production_orders IS 'Ordens de produção - suporta INSERT e UPDATE';
COMMENT ON TABLE bronze.maintenance_orders IS 'Ordens de manutenção - suporta INSERT e UPDATE';
COMMENT ON TABLE bronze.quality_inspections IS 'Inspeções de qualidade - suporta INSERT e UPDATE';

-- ============================================================
-- Função auxiliar para converter timestamps em múltiplos formatos
-- ============================================================
CREATE OR REPLACE FUNCTION bronze.try_parse_timestamp(ts_text TEXT)
RETURNS TIMESTAMP AS $$
DECLARE
    formats TEXT[] := ARRAY[
        'YYYY-MM-DD HH24:MI:SS',
        'YYYY-MM-DD',
        'YYYY/MM/DD HH24:MI:SS',
        'YYYY/MM/DD',
        'DD/MM/YYYY HH24:MI:SS',
        'DD/MM/YYYY',
        'DD-MM-YYYY HH24:MI:SS',
        'DD-MM-YYYY',
        'YYYY-MM-DD"T"HH24:MI:SS',
        'YYYY-MM-DD"T"HH24:MI:SS"Z"'
    ];
    fmt TEXT;
BEGIN
    IF ts_text IS NULL OR ts_text = '' THEN
        RETURN NULL;
    END IF;
    
    FOREACH fmt IN ARRAY formats
    LOOP
        BEGIN
            RETURN TO_TIMESTAMP(ts_text, fmt);
        EXCEPTION WHEN OTHERS THEN
            CONTINUE;
        END;
    END LOOP;
    
    -- Se nenhum formato funcionou, retorna NULL
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


