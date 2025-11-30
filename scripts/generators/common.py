"""
Funções compartilhadas para geradores de dados.
"""

import os
import random
import string
from datetime import datetime, timedelta
from typing import Optional
from faker import Faker
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# Exportar RealDictCursor para uso nos geradores
__all__ = [
    "get_db_connection",
    "random_status_inconsistent",
    "random_date_between",
    "random_date_mixed_formats",
    "maybe_stringify_number",
    "alnum",
    "random_bool_inconsistent",
    "execute_upsert",
    "execute_batch_insert",
    "RealDictCursor",
    "EQUIPMENT_STATUSES",
    "PRODUCTION_STATUSES",
    "MAINTENANCE_STATUSES",
    "MAINTENANCE_TYPES",
    "MAINTENANCE_PRIORITIES",
    "SENSOR_TYPES",
    "DEFECT_CODES",
    "EQUIPMENT_TYPES",
    "MANUFACTURERS",
    "LOCATIONS",
    "P_STRING_DATE_IN_FIELDS",
    "P_STRING_NUMERIC_IN_FIELDS",
    "P_NULL_LOCATION",
    "fake",
]

# Carregar variáveis de ambiente
load_dotenv()

# ===== Constantes =====
EQUIPMENT_STATUSES = ["operational", "maintenance", "idle", "broken", "retired"]
PRODUCTION_STATUSES = ["planned", "in_progress", "completed", "cancelled", "on_hold"]
MAINTENANCE_STATUSES = ["scheduled", "in_progress", "completed", "cancelled"]
MAINTENANCE_TYPES = ["preventive", "corrective", "predictive", "emergency"]
MAINTENANCE_PRIORITIES = ["low", "medium", "high", "critical"]
SENSOR_TYPES = ["temperature", "vibration", "pressure", "humidity", "current"]
DEFECT_CODES = ["D001", "D002", "D003", "D004", "D005", "D006"]

EQUIPMENT_TYPES = ["CNC", "Press", "Welder", "Assembly", "Packaging", "Quality Control"]
MANUFACTURERS = ["Siemens", "ABB", "Fanuc", "Bosch", "Schneider", "Rockwell"]
LOCATIONS = ["Linha 1", "Linha 2", "Linha 3", "Almoxarifado", "Manutenção", "Qualidade"]

# Percentuais de problemas (configuráveis)
P_STATUS_CASE_VARIATION = 0.25
P_STRING_DATE_IN_FIELDS = 0.50
P_STRING_NUMERIC_IN_FIELDS = 0.15
P_NULL_LOCATION = 0.05

# Inicializar Faker
fake = Faker("pt_BR")


# ===== Funções de Conexão =====
def get_db_connection():
    """Cria conexão com PostgreSQL usando variáveis de ambiente."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "manufatura_raw"),
        user=os.getenv("POSTGRES_USER", "user"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
    )


# ===== Funções de Geração de Dados =====
def random_status_inconsistent(statuses: list, p_variation: float = P_STATUS_CASE_VARIATION) -> str:
    """Gera status com variações de case/formato."""
    s = random.choice(statuses)
    if random.random() < p_variation:
        choices = [s.upper(), s.capitalize(), s.lower(), s.replace("_", " "), s.replace("_", "-")]
        s = random.choice(choices)
    return s


def random_date_between(days_back: int = 365) -> datetime:
    """Gera data aleatória nos últimos N dias."""
    base = datetime.utcnow()
    delta = timedelta(days=random.randint(0, days_back), seconds=random.randint(0, 86399))
    return base - delta


def random_date_mixed_formats(dt: datetime) -> str:
    """Formata data em formato aleatório (simula inconsistências)."""
    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]
    return dt.strftime(random.choice(formats))


def maybe_stringify_number(x, p: float = P_STRING_NUMERIC_IN_FIELDS) -> str:
    """Converte número para string com probabilidade p."""
    if random.random() < p:
        return f"{x}"
    return x


def alnum(n: int = 8) -> str:
    """Gera string alfanumérica aleatória."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))


def random_bool_inconsistent() -> str:
    """Gera booleano em formato inconsistente (string)."""
    opts = ["true", "1", "yes", "Y", "false", "0", "no", "N"]
    if random.random() < 0.3:
        return str(random.choice(opts))
    return random.choice(["true", "false"])


# ===== Funções de Upsert =====
def execute_upsert(
    conn,
    table: str,
    data: dict,
    primary_key: str,
    mode: str = "upsert"
):
    """
    Executa INSERT, UPDATE ou UPSERT em tabela PostgreSQL.
    
    Args:
        conn: Conexão PostgreSQL
        table: Nome da tabela (com schema, ex: 'bronze.equipment_master')
        data: Dicionário com dados (chaves = colunas)
        primary_key: Nome da coluna chave primária
        mode: 'insert', 'update' ou 'upsert'
    """
    cursor = conn.cursor()
    
    if mode == "insert":
        # INSERT simples
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        values = tuple(data.values())
        
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, values)
        
    elif mode == "update":
        # UPDATE por primary_key
        set_clause = ", ".join([f"{k} = %s" for k in data.keys() if k != primary_key])
        values = tuple([v for k, v in data.items() if k != primary_key] + [data[primary_key]])
        
        query = f"UPDATE {table} SET {set_clause} WHERE {primary_key} = %s"
        cursor.execute(query, values)
        
    elif mode == "upsert":
        # INSERT ... ON CONFLICT DO UPDATE (PostgreSQL)
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        set_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in data.keys() if k != primary_key])
        values = tuple(data.values())
        
        query = f"""
            INSERT INTO {table} ({columns}) 
            VALUES ({placeholders})
            ON CONFLICT ({primary_key}) 
            DO UPDATE SET {set_clause}
        """
        cursor.execute(query, values)
    
    conn.commit()
    cursor.close()


def execute_batch_insert(
    conn,
    table: str,
    data_list: list[dict],
    batch_size: int = 1000
):
    """
    Executa INSERT em lote para melhor performance.
    
    Args:
        conn: Conexão PostgreSQL
        table: Nome da tabela
        data_list: Lista de dicionários com dados
        batch_size: Tamanho do lote
    """
    if not data_list:
        return
    
    cursor = conn.cursor()
    columns = ", ".join(data_list[0].keys())
    placeholders = ", ".join(["%s"] * len(data_list[0]))
    
    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    
    # Processar em lotes
    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i + batch_size]
        values = [tuple(row.values()) for row in batch]
        cursor.executemany(query, values)
    
    conn.commit()
    cursor.close()

