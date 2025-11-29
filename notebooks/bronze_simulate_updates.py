"""
Script para simular atualizações incrementais nos dados Bronze.

Uso:
1. Execute bronze_layer.ipynb (carga inicial)
2. Execute silver_layer_full_load.ipynb (primeira carga completa)
3. Execute este script para simular atualizações
4. Execute silver_layer.ipynb (processar apenas atualizações)
"""

from pyspark.sql import functions as F, types as T
from pyspark.sql import Row
from datetime import datetime, timedelta
from faker import Faker
import random
import string

# ===== Parâmetros =====
CATALOG = "manufatura_lakehouse"
SCHEMA  = "bronze"
SEED    = 123  # Seed diferente para gerar dados diferentes

# Quantidades para simular atualizações
N_NEW_EQUIPMENTS        = 5
N_UPDATE_EQUIPMENTS     = 10
N_NEW_IOT_READINGS      = 1000
N_NEW_PRODUCTION_ORDERS = 50
N_NEW_MAINTENANCE_ORDERS = 20
N_NEW_QUALITY_INSPECTIONS = 30

# Percentuais
P_STATUS_CASE_VARIATION    = 0.25
P_STRING_DATE_IN_FIELDS    = 0.50
P_STRING_NUMERIC_IN_FIELDS = 0.15
P_NULL_LOCATION            = 0.05

random.seed(SEED)
fake = Faker("pt_BR")
Faker.seed(SEED)

def fqtn(table):
    if CATALOG:
        return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"
    else:
        return f"`{SCHEMA}`.`{table}`"

EQUIPMENT_STATUSES = ["operational", "maintenance", "idle", "broken", "retired"]
equipment_types = ["CNC", "Press", "Welder", "Assembly", "Packaging", "Quality Control"]
manufacturers = ["Siemens", "ABB", "Fanuc", "Bosch", "Schneider", "Rockwell"]
locations = ["Linha 1", "Linha 2", "Linha 3", "Almoxarifado", "Manutenção", "Qualidade"]
SENSOR_TYPES = ["temperature", "vibration", "pressure", "humidity", "current"]

def random_status_inconsistent(statuses):
    s = random.choice(statuses)
    if random.random() < P_STATUS_CASE_VARIATION:
        choices = [s.upper(), s.capitalize(), s.lower(), s.replace("_", " "), s.replace("_", "-")]
        s = random.choice(choices)
    return s

def random_date_between(days_back=365):
    base = datetime.utcnow()
    delta = timedelta(days=random.randint(0, days_back), seconds=random.randint(0, 86399))
    return base - delta

def random_date_mixed_formats(dt):
    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]
    return dt.strftime(random.choice(formats))

def alnum(n=8):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))

# ============================================
# 1) Simular Atualizações em Equipment Master
# ============================================
print("🔄 Simulando atualizações em equipment_master...")

existing_equipment = spark.sql(f"SELECT equipment_id FROM {fqtn('equipment_master')}").collect()
existing_ids = [r["equipment_id"] for r in existing_equipment[:N_UPDATE_EQUIPMENTS]]

equipment_schema = T.StructType([
    T.StructField("equipment_id",      T.StringType(), True),
    T.StructField("equipment_name",      T.StringType(), True),
    T.StructField("equipment_type",       T.StringType(), True),
    T.StructField("location",             T.StringType(), True),
    T.StructField("installation_date",   T.StringType(), True),
    T.StructField("manufacturer",         T.StringType(), True),
    T.StructField("model",               T.StringType(), True),
    T.StructField("status",              T.StringType(), True),
    T.StructField("last_update_date",    T.StringType(), True),
])

rows = []

# Novos equipamentos
for i in range(N_NEW_EQUIPMENTS):
    eid = f"EQ{20000+i:05d}"
    eq_type = random.choice(equipment_types)
    name = f"{eq_type}-{random.choice(['Alpha','Beta','Gamma','Delta'])}-{alnum(4)}"
    location = None if random.random() < P_NULL_LOCATION else random.choice(locations)
    install_date = random_date_between(2000)
    
    row = Row(
        equipment_id      = eid,
        equipment_name    = name,
        equipment_type    = eq_type,
        location          = location,
        installation_date = random_date_mixed_formats(install_date) if random.random() < P_STRING_DATE_IN_FIELDS else install_date.strftime("%Y-%m-%d"),
        manufacturer      = random.choice(manufacturers),
        model             = f"MOD-{random.randint(100,999)}",
        status            = random_status_inconsistent(EQUIPMENT_STATUSES),
        last_update_date  = random_date_mixed_formats(datetime.utcnow() - timedelta(hours=random.randint(0, 24)))
    )
    rows.append(row)

# Atualizações em equipamentos existentes
for eid in existing_ids:
    new_status = random_status_inconsistent(EQUIPMENT_STATUSES)
    new_location = random.choice(locations) if random.random() > 0.3 else None
    
    original = spark.sql(f"SELECT * FROM {fqtn('equipment_master')} WHERE equipment_id = '{eid}'").first()
    
    row = Row(
        equipment_id      = eid,
        equipment_name    = original["equipment_name"],
        equipment_type    = original["equipment_type"],
        location          = new_location if new_location else original["location"],
        installation_date = original["installation_date"],
        manufacturer      = original["manufacturer"],
        model             = original["model"],
        status            = new_status,
        last_update_date  = random_date_mixed_formats(datetime.utcnow() - timedelta(hours=random.randint(0, 6)))
    )
    rows.append(row)

df_updates = spark.createDataFrame(rows, schema=equipment_schema)
df_updates.write.mode("append").format("delta").saveAsTable(fqtn("equipment_master"))

print(f"✅ Inseridos {len(rows)} registros em equipment_master")
print(f"   - {N_NEW_EQUIPMENTS} novos equipamentos")
print(f"   - {N_UPDATE_EQUIPMENTS} atualizações de equipamentos existentes")

# ============================================
# 2) Simular Novas Leituras IoT
# ============================================
print("\n🔄 Simulando novas leituras IoT...")

equipment_ids = [r["equipment_id"] for r in spark.sql(f"SELECT DISTINCT equipment_id FROM {fqtn('equipment_master')}").collect()]

sensor_ranges = {
    "temperature": (20, 100),
    "vibration": (0, 50),
    "pressure": (0, 10),
    "humidity": (30, 90),
    "current": (0, 100)
}

units_map = {
    "temperature": "°C",
    "vibration": "mm/s",
    "pressure": "bar",
    "humidity": "%",
    "current": "A"
}

iot_schema = T.StructType([
    T.StructField("reading_id",        T.StringType(), True),
    T.StructField("equipment_id",      T.StringType(), True),
    T.StructField("sensor_id",         T.StringType(), True),
    T.StructField("sensor_type",       T.StringType(), True),
    T.StructField("reading_value",     T.StringType(), True),
    T.StructField("reading_timestamp", T.StringType(), True),
    T.StructField("unit",              T.StringType(), True),
])

rows = []
for i in range(N_NEW_IOT_READINGS):
    eq_id = random.choice(equipment_ids)
    sensor_type = random.choice(SENSOR_TYPES)
    sensor_id = f"SENS-{eq_id}-{sensor_type[:3].upper()}-{random.randint(1,5)}"
    
    min_val, max_val = sensor_ranges[sensor_type]
    value = random.uniform(min_val, max_val)
    
    timestamp = datetime.utcnow() - timedelta(hours=random.randint(0, 24), minutes=random.randint(0, 59))
    reading_ts = random_date_mixed_formats(timestamp) if random.random() < 0.3 else timestamp.strftime("%Y-%m-%d %H:%M:%S")
    
    row = Row(
        reading_id        = f"IOT{2000000+i:08d}",
        equipment_id      = eq_id,
        sensor_id         = sensor_id,
        sensor_type       = sensor_type,
        reading_value     = str(round(value, 2)),
        reading_timestamp = reading_ts,
        unit              = units_map[sensor_type]
    )
    rows.append(row)

df_iot_updates = spark.createDataFrame(rows, schema=iot_schema)
df_iot_updates.write.mode("append").format("delta").saveAsTable(fqtn("iot_sensor_readings"))

print(f"✅ Inseridas {N_NEW_IOT_READINGS} novas leituras IoT (últimas 24h)")

# ============================================
# Resumo
# ============================================
print("\n" + "=" * 60)
print("RESUMO DAS ATUALIZAÇÕES SIMULADAS")
print("=" * 60)

tables = ["equipment_master", "iot_sensor_readings", "production_orders", 
          "maintenance_orders", "quality_inspections"]

for table in tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {fqtn(table)}").first()["cnt"]
    print(f"✅ {table}: {count:,} registros totais")

print("\n💡 Próximo passo: Execute silver_layer.ipynb para processar apenas os dados novos/atualizados")

