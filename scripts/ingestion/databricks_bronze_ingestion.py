#!/usr/bin/env python3
"""
Script de ingestão de dados do PostgreSQL local para Databricks Bronze.

Suporta duas estratégias:
1. databricks-sql-connector (INSERT direto via SQL) - padrão
2. PySpark (Delta Lake via JDBC) - opcional

Uso:
    python databricks_bronze_ingestion.py --table equipment_master
    python databricks_bronze_ingestion.py --table all
    python databricks_bronze_ingestion.py --table iot_sensor_readings --method spark --days-back 7
"""

import argparse
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# Adicionar diretório scripts ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import (
    print_header,
    print_info,
    print_success,
    print_warning,
    print_error,
    print_summary_table,
    logger,
)
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table
from rich import box

# Carregar variáveis de ambiente
load_dotenv()

# Tabelas Bronze disponíveis
BRONZE_TABLES = [
    "equipment_master",
    "iot_sensor_readings",
    "production_orders",
    "maintenance_orders",
    "quality_inspections",
]

# Mapeamento de colunas de timestamp para watermark
TIMESTAMP_COLUMNS = {
    "equipment_master": "last_update_date",
    "iot_sensor_readings": "reading_timestamp",
    "production_orders": "last_update",
    "maintenance_orders": "last_update",
    "quality_inspections": "last_update",
}


def get_postgres_connection():
    """Cria conexão com PostgreSQL local."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "manufatura_raw"),
        user=os.getenv("POSTGRES_USER", "user"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
    )


def get_databricks_connection():
    """Cria conexão com Databricks usando databricks-sql-connector."""
    try:
        from databricks import sql
    except ImportError:
        raise ImportError(
            "databricks-sql-connector não está instalado. "
            "Instale com: uv pip install databricks-sql-connector"
        )
    
    connection_params = {
        "server_hostname": os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
        "access_token": os.getenv("DATABRICKS_ACCESS_TOKEN"),
    }
    
    # Validar parâmetros obrigatórios
    missing = [k for k, v in connection_params.items() if not v]
    if missing:
        raise ValueError(
            f"Variáveis de ambiente faltando: {', '.join(missing)}. "
            f"Configure no arquivo .env"
        )
    
    return sql.connect(**connection_params)


def read_from_postgres(
    table: str,
    is_full_load: bool = False,
    days_back: int = 90
) -> List[Dict]:
    """
    Lê dados do PostgreSQL local.
    
    Args:
        table: Nome da tabela (sem schema)
        is_full_load: Se True, lê todos os dados. Se False, usa incremental com watermark
        days_back: Quantos dias atrás buscar (para incremental)
    
    Returns:
        Lista de dicionários com os dados
    """
    conn = get_postgres_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    schema_table = f"bronze.{table}"
    
    if is_full_load:
        query = f"SELECT * FROM {schema_table}"
    else:
        # Incremental: usar watermark
        timestamp_col = TIMESTAMP_COLUMNS.get(table)
        if not timestamp_col:
            logger.warning(f"Tabela {table} não tem coluna de timestamp configurada. Usando full load.")
            query = f"SELECT * FROM {schema_table}"
        else:
            # Buscar registros dos últimos N dias ou NULL
            cutoff = datetime.utcnow() - timedelta(days=days_back)
            query = f"""
                SELECT * FROM {schema_table}
                WHERE {timestamp_col} IS NULL 
                   OR {timestamp_col} >= %s
            """
            cursor.execute(query, (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            return [dict(row) for row in results]
    
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return [dict(row) for row in results]


def write_to_databricks_sql(table: str, data: List[Dict]) -> int:
    """
    Escreve dados no Databricks usando databricks-sql-connector (INSERT direto).
    
    Args:
        table: Nome da tabela (sem schema)
        data: Lista de dicionários com dados
    
    Returns:
        Número de registros inseridos
    """
    if not data:
        return 0
    
    conn = get_databricks_connection()
    cursor = conn.cursor()
    
    # Schema e catalog do Databricks (configurável via env)
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    full_table = f"{catalog}.{schema}.{table}"
    
    # Preparar INSERT
    columns = list(data[0].keys())
    # Databricks SQL usa ? como placeholder (ou %s com use_inline_params=True)
    placeholders = ", ".join(["?"] * len(columns))
    columns_str = ", ".join(columns)
    
    query = f"INSERT INTO {full_table} ({columns_str}) VALUES ({placeholders})"
    
    # Inserir em lotes
    batch_size = 1000
    total_inserted = 0
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        values = [tuple(row[col] for col in columns) for row in batch]
        cursor.executemany(query, values)
        total_inserted += len(batch)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return total_inserted


def write_to_databricks_spark(table: str, data: List[Dict]) -> int:
    """
    Escreve dados no Databricks usando PySpark (Delta Lake).
    
    Args:
        table: Nome da tabela (sem schema)
        data: Lista de dicionários com dados
    
    Returns:
        Número de registros inseridos
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType
    except ImportError:
        raise ImportError(
            "PySpark não está instalado. "
            "Instale com: uv pip install pyspark delta-spark"
        )
    
    if not data:
        return 0
    
    # Configurar Spark
    spark = SparkSession.builder \
        .appName("DatabricksBronzeIngestion") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Criar DataFrame
    # Inferir schema (todos STRING para Bronze)
    schema = StructType([
        StructField(col, StringType(), True) 
        for col in data[0].keys()
    ])
    
    df = spark.createDataFrame(data, schema=schema)
    
    # Escrever no Databricks (Delta Lake)
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    full_table = f"{catalog}.{schema}.{table}"
    
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(full_table)
    
    count = df.count()
    spark.stop()
    
    return count


def create_bronze_table_if_not_exists(table: str) -> None:
    """
    Cria a tabela Bronze no Databricks se ela não existir.
    
    Args:
        table: Nome da tabela (sem schema)
    """
    conn = get_databricks_connection()
    cursor = conn.cursor()
    
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    
    # Criar schema se não existir
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.commit()
    except Exception as e:
        logger.debug(f"Schema {schema} já existe ou erro ao criar: {e}")
    
    # Definir schemas das tabelas
    table_schemas = {
        "equipment_master": """
            CREATE TABLE IF NOT EXISTS {full_table} (
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
            )
        """,
        "iot_sensor_readings": """
            CREATE TABLE IF NOT EXISTS {full_table} (
                reading_id        STRING,
                equipment_id      STRING,
                sensor_id         STRING,
                sensor_type       STRING,
                reading_value     STRING,
                reading_timestamp STRING,
                unit              STRING
            )
            USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
        """,
        "production_orders": """
            CREATE TABLE IF NOT EXISTS {full_table} (
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
            )
        """,
        "maintenance_orders": """
            CREATE TABLE IF NOT EXISTS {full_table} (
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
            )
        """,
        "quality_inspections": """
            CREATE TABLE IF NOT EXISTS {full_table} (
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
            )
        """
    }
    
    if table not in table_schemas:
        logger.warning(f"Schema não definido para tabela {table}. Pulando criação.")
        cursor.close()
        conn.close()
        return
    
    full_table = f"{catalog}.{schema}.{table}"
    create_sql = table_schemas[table].format(full_table=full_table)
    
    try:
        logger.info(f"Criando tabela {full_table} se não existir...")
        cursor.execute(create_sql)
        conn.commit()
        logger.info(f"Tabela {full_table} criada ou já existe.")
    except Exception as e:
        logger.error(f"Erro ao criar tabela {full_table}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def check_table_exists_in_databricks(table: str) -> bool:
    """
    Verifica se a tabela existe no Databricks e se tem dados.
    
    Returns:
        True se a tabela existe e tem dados, False caso contrário
    """
    try:
        conn = get_databricks_connection()
        cursor = conn.cursor()
        
        catalog = os.getenv("DATABRICKS_CATALOG", "main")
        schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
        full_table = f"{catalog}.{schema}.{table}"
        
        # Verificar se a tabela existe e tem dados
        # Usar INFORMATION_SCHEMA para verificar existência primeiro
        check_query = f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
              AND table_name = '{table}'
        """
        cursor.execute(check_query)
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            cursor.close()
            conn.close()
            return False
        
        # Se existe, verificar se tem dados
        count_query = f"SELECT COUNT(*) as cnt FROM {full_table} LIMIT 1"
        try:
            cursor.execute(count_query)
            result = cursor.fetchone()
            count = result[0] if result else 0
            has_data = count > 0
        except Exception as e:
            # Erro ao contar (pode ser permissão ou tabela vazia)
            logger.debug(f"Erro ao verificar dados da tabela {table}: {e}")
            has_data = False
        
        cursor.close()
        conn.close()
        return has_data
    except Exception as e:
        # Se não conseguir conectar, assume que não existe (primeira carga)
        logger.debug(f"Não foi possível verificar tabela no Databricks: {e}. Assumindo primeira carga.")
        return False


def ingest_table(
    table: str,
    method: str = "sql",
    days_back: int = 90
) -> int:
    """
    Ingere uma tabela do PostgreSQL para Databricks.
    
    Detecta automaticamente se é primeira carga (tabela não existe/vazia) e usa full load.
    Caso contrário, usa incremental com watermark dos últimos N dias.
    
    Args:
        table: Nome da tabela
        method: 'sql' (databricks-sql-connector) ou 'spark' (PySpark)
        days_back: Quantos dias atrás buscar (para incremental, se não for primeira carga)
    
    Returns:
        Número de registros ingeridos
    """
    # Detectar primeira carga: se tabela não existe/vazia, usa full load
    table_exists = check_table_exists_in_databricks(table)
    is_full_load = not table_exists
    actual_mode = "full" if is_full_load else "incremental"
    
    if is_full_load:
        logger.info(f"Tabela {table} não existe ou está vazia no Databricks. Primeira carga: usando full load.")
    else:
        logger.info(f"Tabela {table} existe no Databricks. Usando incremental (últimos {days_back} dias).")
    
    logger.info(f"Lendo dados de bronze.{table} (modo: {actual_mode})...")
    start_time = datetime.now()
    
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        ) as progress:
            task = progress.add_task("Lendo do PostgreSQL...", total=None)
            data = read_from_postgres(table, is_full_load=is_full_load, days_back=days_back)
            progress.update(task, completed=True)
        
        read_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"{len(data)} registros lidos do PostgreSQL em {read_time:.2f}s")
        
        if not data:
            print_warning(f"Nenhum dado novo para ingerir em {table}")
            return 0
        
        logger.info(f"Escrevendo dados no Databricks (método: {method})...")
        write_start = datetime.now()
        
        if method == "sql":
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            ) as progress:
                task = progress.add_task("Inserindo no Databricks...", total=len(data))
                count = write_to_databricks_sql(table, data)
                progress.update(task, completed=len(data))
        elif method == "spark":
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
            ) as progress:
                task = progress.add_task("Processando com Spark...", total=None)
                count = write_to_databricks_spark(table, data)
                progress.update(task, completed=True)
        else:
            raise ValueError(f"Método inválido: {method}. Use 'sql' ou 'spark'")
        
        write_time = (datetime.now() - write_start).total_seconds()
        total_time = (datetime.now() - start_time).total_seconds()
        
        print_success(f"{count} registros inseridos no Databricks")
        
        # Resumo da tabela
        print_summary_table({
            "Tabela": table,
            "Modo": actual_mode,
            "Método": method,
            "Registros lidos": len(data),
            "Registros inseridos": count,
            "Tempo leitura": f"{read_time:.2f}s",
            "Tempo escrita": f"{write_time:.2f}s",
            "Tempo total": f"{total_time:.2f}s",
            "Taxa": f"{count/total_time:.1f} registros/s" if total_time > 0 else "N/A"
        }, title=f"Resumo - {table}")
        
        return count
        
    except Exception as e:
        logger.error(f"Erro ao ingerir {table}: {e}", exc_info=True)
        print_error(f"Falha ao ingerir {table}: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Ingestão de dados do PostgreSQL local para Databricks Bronze"
    )
    parser.add_argument(
        "--table",
        type=str,
        required=True,
        help=f"Nome da tabela ou 'all' para todas. Tabelas: {', '.join(BRONZE_TABLES)}"
    )
    parser.add_argument(
        "--method",
        type=str,
        choices=["sql", "spark"],
        default="sql",
        help="Método de escrita: sql (databricks-sql-connector) ou spark (PySpark)"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=90,
        help="Quantos dias atrás buscar (para incremental, após primeira carga)"
    )
    
    args = parser.parse_args()
    
    # Validar tabela
    if args.table == "all":
        tables = BRONZE_TABLES
    elif args.table in BRONZE_TABLES:
        tables = [args.table]
    else:
        print_error(f"Tabela inválida: {args.table}")
        logger.error(f"Tabelas disponíveis: {', '.join(BRONZE_TABLES)}")
        sys.exit(1)
    
    # Header
    print_header(
        "📤 Databricks Bronze Ingestion",
        f"Tabelas: {len(tables)} | Método: {args.method} | Days back: {args.days_back}"
    )
    
    start_time = datetime.now()
    total_inserted = 0
    results = []
    
    for i, table in enumerate(tables, 1):
        logger.info(f"[{i}/{len(tables)}] Processando {table}...")
        try:
            count = ingest_table(table, method=args.method, days_back=args.days_back)
            total_inserted += count
            results.append({"tabela": table, "registros": count, "status": "✅ Sucesso"})
        except Exception as e:
            logger.error(f"Falha ao ingerir {table}: {e}")
            results.append({"tabela": table, "registros": 0, "status": f"❌ Erro: {str(e)[:50]}"})
            if args.table != "all":
                sys.exit(1)
    
    total_time = (datetime.now() - start_time).total_seconds()
    
    # Resumo final
    print_header("📊 Resumo Final da Ingestão")
    
    summary_table = Table(show_header=True, header_style="bold cyan", box=box.ROUNDED)
    summary_table.add_column("Tabela", style="cyan")
    summary_table.add_column("Registros", justify="right", style="green")
    summary_table.add_column("Status", style="yellow")
    
    for result in results:
        summary_table.add_row(
            result["tabela"],
            str(result["registros"]),
            result["status"]
        )
    
    summary_table.add_row("", "", "", style="bold")
    summary_table.add_row(
        "[bold]TOTAL[/bold]",
        f"[bold green]{total_inserted}[/bold green]",
        f"[bold]Tempo: {total_time:.2f}s[/bold]"
    )
    
    from utils.logger import console
    console.print(summary_table)
    
    print_success(f"Ingestão concluída: {total_inserted} registros inseridos no total em {total_time:.2f}s")


if __name__ == "__main__":
    main()

