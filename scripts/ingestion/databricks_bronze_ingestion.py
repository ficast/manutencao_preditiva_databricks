#!/usr/bin/env python3
"""
Script de ingestão de dados do PostgreSQL local para Databricks Bronze (otimizado).

Principais melhorias:
- Uso de watermark real (MAX(timestamp) no Databricks) para carga incremental
- Uso de staging table + MERGE INTO em vez de UPDATE linha a linha
- Menos roundtrips na conexão Databricks
"""

import argparse
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rich.progress import Progress
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
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
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

# Mapeamento de chaves primárias para MERGE
PRIMARY_KEYS = {
    "equipment_master": "equipment_id",
    "iot_sensor_readings": "reading_id",
    "production_orders": "production_order_id",
    "maintenance_orders": "maintenance_order_id",
    "quality_inspections": "inspection_id",
}


# ---------------------------------------------------------------------------
# Conexões
# ---------------------------------------------------------------------------

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


def get_full_table_name(table: str) -> str:
    """Retorna nome totalmente qualificado da tabela no Databricks."""
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    return f"{catalog}.{schema}.{table}"


def get_schema_name() -> str:
    return os.getenv("DATABRICKS_SCHEMA", "bronze")


# ---------------------------------------------------------------------------
# Utilitários Databricks (existência, watermark, criação de tabelas)
# ---------------------------------------------------------------------------

def check_table_exists_in_databricks(table: str) -> bool:
    """
    Verifica se a tabela existe no Databricks.
    """
    full_table = get_full_table_name(table)
    schema = get_schema_name()

    try:
        with get_databricks_connection() as conn:
            with conn.cursor() as cursor:
                # SHOW TABLES é mais robusto que information_schema em alguns cenários
                show_query = f"SHOW TABLES IN {schema} LIKE '{table}'"
                cursor.execute(show_query)
                row = cursor.fetchone()
                exists = row is not None
                return exists
    except Exception as e:
        logger.debug(f"Não foi possível verificar tabela {full_table}: {e}. Assumindo que não existe.")
        return False

def get_postgres_max_timestamp(table: str) -> Optional[datetime]:
    timestamp_col = TIMESTAMP_COLUMNS.get(table)
    if not timestamp_col:
        return None

    conn = get_postgres_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT MAX({timestamp_col}) FROM bronze.{table}"
        )
        result = cur.fetchone()
        if result and result[0]:
            return result[0]
        return None
    finally:
        cur.close()
        conn.close()


def get_databricks_max_timestamp(table: str) -> Optional[datetime]:
    """
    Obtém o MAX(timestamp) da tabela Bronze no Databricks para usar como watermark.
    """
    timestamp_col = TIMESTAMP_COLUMNS.get(table)
    if not timestamp_col:
        return None

    full_table = get_full_table_name(table)

    try:
        with get_databricks_connection() as conn:
            with conn.cursor() as cursor:
                query = f"SELECT MAX({timestamp_col}) FROM {full_table}"
                cursor.execute(query)
                result = cursor.fetchone()
                max_ts = result[0] if result else None

                if max_ts is None:
                    return None

                # databricks-sql-connector pode retornar str ou datetime
                if isinstance(max_ts, datetime):
                    return max_ts
                if isinstance(max_ts, str):
                    # tenta parsear ISO8601
                    try:
                        return datetime.fromisoformat(max_ts.replace("Z", ""))
                    except Exception:
                        logger.warning(f"Não foi possível parsear MAX({timestamp_col})='{max_ts}' como datetime.")
                        return None

                return None
    except Exception as e:
        logger.debug(f"Erro ao buscar watermark para {table}: {e}")
        return None


def create_bronze_table_if_not_exists(table: str) -> None:
    """
    Cria a tabela Bronze no Databricks se ela não existir.
    (mantive a tua definição, mas aqui é o lugar ideal para melhorar tipos)
    """
    from textwrap import dedent

    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")

    table_schemas = {
        "equipment_master": dedent("""
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
        """),
        "iot_sensor_readings": dedent("""
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
        """),
        "production_orders": dedent("""
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
        """),
        "maintenance_orders": dedent("""
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
        """),
        "quality_inspections": dedent("""
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
        """),
    }

    if table not in table_schemas:
        logger.warning(f"Schema não definido para tabela {table}. Pulando criação.")
        return

    full_table = f"{catalog}.{schema}.{table}"
    create_sql = table_schemas[table].format(full_table=full_table)

    try:
        with get_databricks_connection() as conn:
            with conn.cursor() as cursor:
                # Criar schema se não existir (sem catalogo explicitado para compatibilidade)
                try:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                except Exception as e:
                    logger.debug(f"Schema {schema} já existe ou erro ao criar: {e}")

                logger.info(f"Criando tabela {full_table} se não existir...")
                cursor.execute(create_sql)
                logger.info(f"Tabela {full_table} criada ou já existe.")
    except Exception as e:
        logger.error(f"Erro ao criar tabela {full_table}: {e}")
        raise


def create_staging_table_if_not_exists(table: str) -> str:
    """
    Cria uma staging table Delta para usar em operações de MERGE.
    """
    full_table = get_full_table_name(table)
    staging_table = full_table + "__staging"

    try:
        with get_databricks_connection() as conn:
            with conn.cursor() as cursor:
                # Cria staging com o mesmo schema da tabela principal
                create_sql = f"CREATE TABLE IF NOT EXISTS {staging_table} LIKE {full_table}"
                cursor.execute(create_sql)
                # Garante que staging esteja vazia antes de usar
                cursor.execute(f"TRUNCATE TABLE {staging_table}")
        return staging_table
    except Exception as e:
        logger.error(f"Erro ao criar staging table {staging_table}: {e}")
        raise


# ---------------------------------------------------------------------------
# Leitura Postgres
# ---------------------------------------------------------------------------

def read_from_postgres(
    table: str,
    is_full_load: bool = False,
    days_back: int = 90,
    watermark: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """
    Lê dados do PostgreSQL local.

    Se watermark for informado, ele é usado diretamente.
    Caso contrário, usa is_full_load/days_back.
    """
    conn = get_postgres_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    schema_table = f"bronze.{table}"

    try:
        if is_full_load:
            query = f"SELECT * FROM {schema_table}"
            cursor.execute(query)
        else:
            timestamp_col = TIMESTAMP_COLUMNS.get(table)
            if not timestamp_col:
                logger.warning(
                    f"Tabela {table} não tem coluna de timestamp configurada. Usando full load."
                )
                query = f"SELECT * FROM {schema_table}"
                cursor.execute(query)
            else:
                if watermark is None:
                    cutoff = datetime.utcnow() - timedelta(days=days_back)
                    logger.info(
                        f"Usando cutoff de {days_back} dias para {table}: {cutoff.isoformat()}"
                    )
                else:
                    cutoff = watermark
                    logger.info(
                        f"Usando watermark do Databricks para {table}: {cutoff.isoformat()}"
                    )

                query = f"""
                    SELECT * FROM {schema_table}
                    WHERE {timestamp_col} IS NULL
                       OR {timestamp_col} > %s
                """
                cursor.execute(query, (cutoff,))
        results = cursor.fetchall()
        return [dict(row) for row in results]
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# Escrita Databricks (otimizada com staging + MERGE)
# ---------------------------------------------------------------------------

def write_to_databricks_sql(
    table: str,
    data: List[Dict[str, Any]],
    is_full_load: bool,
    progress: Optional["Progress"] = None,
    task_id: Optional[int] = None,
) -> int:
    """
    Escreve dados no Databricks usando databricks-sql-connector.
    - Full load: INSERT direto
    - Incremental: staging + MERGE
    """
    if not data:
        return 0

    full_table = get_full_table_name(table)
    primary_key = PRIMARY_KEYS.get(table)
    columns = list(data[0].keys())
    batch_size = 50

    total_processed = 0

    with get_databricks_connection() as conn:
        with conn.cursor() as cursor:
            try:
                # FULL LOAD
                if is_full_load or not primary_key or primary_key not in columns:
                    logger.info(f"Modo full load para {table}. INSERT direto.")

                    placeholders = ", ".join(["?"] * len(columns))
                    columns_str = ", ".join(columns)
                    insert_query = f"INSERT INTO {full_table} ({columns_str}) VALUES ({placeholders})"

                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        values = [tuple(row.get(col) for col in columns) for row in batch]
                        cursor.executemany(insert_query, values)

                        total_processed += len(batch)
                        if progress and task_id:
                            progress.update(task_id, completed=total_processed)

                    print_success(f"INSERT FULL LOAD concluído para {table}. Registros: {total_processed}")
                    return total_processed

                # INCREMENTAL (MERGE)
                logger.info(f"Modo incremental para {table}: staging + MERGE INTO.")

                staging_table = create_staging_table_if_not_exists(table)

                # INSERT na staging
                placeholders = ", ".join(["?"] * len(columns))
                columns_str = ", ".join(columns)
                insert_staging = f"INSERT INTO {staging_table} ({columns_str}) VALUES ({placeholders})"

                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    values = [tuple(row.get(col) for col in columns) for row in batch]
                    cursor.executemany(insert_staging, values)

                    total_processed += len(batch)
                    if progress and task_id:
                        progress.update(task_id, completed=total_processed)

                # MERGE INTO
                non_pk_cols = [c for c in columns if c != primary_key]
                set_clause = ", ".join([f"t.{c} = s.{c}" for c in non_pk_cols])
                insert_cols = ", ".join(columns)
                insert_vals = ", ".join([f"s.{c}" for c in columns])

                merge_sql = f"""
                    MERGE INTO {full_table} t
                    USING {staging_table} s
                    ON t.{primary_key} = s.{primary_key}
                    WHEN MATCHED THEN UPDATE SET {set_clause}
                    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
                """

                cursor.execute(merge_sql)
                cursor.execute(f"TRUNCATE TABLE {staging_table}")

                print_success(
                    f"MERGE concluído para {table}. Registros processados: {total_processed}"
                )

                return total_processed

            except Exception as e:
                logger.error(f"Erro ao escrever no Databricks ({table}): {e}")
                raise


# ---------------------------------------------------------------------------
# Orquestração de ingestão por tabela
# ---------------------------------------------------------------------------

def ingest_table(
    table: str,
    days_back: int = 90,
) -> int:
    """
    Ingere uma tabela do PostgreSQL para Databricks.

    - Se tabela não existe: full load
    - Se existe: incremental usando watermark (MAX(timestamp) no Databricks)
    """
    table_exists = check_table_exists_in_databricks(table)

    postgres_max = get_postgres_max_timestamp(table)
    databricks_max = get_databricks_max_timestamp(table)

    if databricks_max and postgres_max and postgres_max <= databricks_max:
        logger.info(f"No new data for {table}. Skipping ingestion.")
        return 0


    is_full_load = not table_exists

    if not table_exists:
        logger.info(f"Tabela {table} não existe no Databricks. Primeira carga (full load).")
    else:
        logger.info(f"Tabela {table} existe no Databricks. Usando modo incremental.")

    # Garante que a tabela exista com o schema correto
    create_bronze_table_if_not_exists(table)

    # Watermark (apenas incremental)
    watermark = None
    if not is_full_load:
        watermark = get_databricks_max_timestamp(table)
        if watermark:
            logger.info(f"Watermark atual no Databricks para {table}: {watermark.isoformat()}")
        else:
            logger.info(
                f"Nenhum watermark válido encontrado para {table}. "
                f"Usando fallback de {days_back} dias."
            )

    logger.info(f"Lendo dados de bronze.{table} (full_load={is_full_load})...")
    start_time = datetime.now()

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        ) as progress:
            task = progress.add_task("Lendo do PostgreSQL...", total=None)
            data = read_from_postgres(
                table,
                is_full_load=is_full_load,
                days_back=days_back,
                watermark=watermark,
            )
            progress.update(task, completed=True)

        read_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"{len(data)} registros lidos do PostgreSQL em {read_time:.2f}s")

        if not data:
            print_warning(f"Nenhum dado novo para ingerir em {table}")
            return 0

        logger.info(f"Escrevendo dados no Databricks para {table}...")
        write_start = datetime.now()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        ) as progress:
            task = progress.add_task(
                "Inserindo/MERGE no Databricks...", total=len(data)
            )
            count = write_to_databricks_sql(
                table,
                data,
                is_full_load=is_full_load,
                progress=progress,
                task_id=task,
            )

        write_time = (datetime.now() - write_start).total_seconds()
        total_time = (datetime.now() - start_time).total_seconds()

        print_success(f"{count} registros processados no Databricks ({table})")

        print_summary_table(
            {
                "Tabela": table,
                "Modo": "full" if is_full_load else "incremental",
                "Registros lidos": len(data),
                "Registros processados": count,
                "Tempo leitura": f"{read_time:.2f}s",
                "Tempo escrita": f"{write_time:.2f}s",
                "Tempo total": f"{total_time:.2f}s",
                "Taxa": f"{count / total_time:.1f} registros/s" if total_time > 0 else "N/A",
            },
            title=f"Resumo - {table}",
        )

        return count

    except Exception as e:
        logger.error(f"Erro ao ingerir {table}: {e}", exc_info=True)
        print_error(f"Falha ao ingerir {table}: {e}")
        raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Ingestão de dados do PostgreSQL local para Databricks Bronze (otimizado)"
    )
    parser.add_argument(
        "--table",
        type=str,
        required=True,
        help=f"Nome da tabela ou 'all' para todas. Tabelas: {', '.join(BRONZE_TABLES)}",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=90,
        help="Fallback de quantos dias atrás buscar (apenas se não houver watermark no Databricks)",
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
        "📤 Databricks Bronze Ingestion (Optimized)",
        f"Tabelas: {len(tables)} | Days back fallback: {args.days_back}",
    )

    start_time = datetime.now()
    total_processed = 0
    results = []

    for i, table in enumerate(tables, 1):
        logger.info(f"[{i}/{len(tables)}] Processando {table}...")
        try:
            count = ingest_table(table, days_back=args.days_back)
            total_processed += count
            results.append(
                {"tabela": table, "registros": count, "status": "✅ Sucesso"}
            )
        except Exception as e:
            logger.error(f"Falha ao ingerir {table}: {e}")
            results.append(
                {
                    "tabela": table,
                    "registros": 0,
                    "status": f"❌ Erro: {str(e)[:80]}",
                }
            )
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
            result["tabela"], str(result["registros"]), result["status"]
        )

    summary_table.add_row("", "", "", style="bold")
    summary_table.add_row(
        "[bold]TOTAL[/bold]",
        f"[bold green]{total_processed}[/bold green]",
        f"[bold]Tempo: {total_time:.2f}s[/bold]",
    )

    from utils.logger import console

    console.print(summary_table)

    print_success(
        f"Ingestão concluída: {total_processed} registros processados no total em {total_time:.2f}s"
    )


if __name__ == "__main__":
    main()
