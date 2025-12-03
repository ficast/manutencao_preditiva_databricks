#!/usr/bin/env python3
"""
Teste que verifica se as tabelas existem no Databricks e compara
a quantidade de dados com o banco PostgreSQL local.
"""

import os
import sys
from pathlib import Path
from typing import Tuple
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# Adicionar diretório scripts ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.ingestion.databricks_bronze_ingestion import (
    get_postgres_connection,
    get_databricks_connection,
    BRONZE_TABLES,
)
from utils.logger import (
    print_header,
    print_info,
    print_success,
    print_warning,
    print_error,
    logger,
)
from rich.table import Table
from rich.console import Console
from rich import box

# Carregar variáveis de ambiente
load_dotenv()


def count_postgres_records(table: str) -> int:
    """Conta registros em uma tabela do PostgreSQL."""
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM bronze.{table}")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception as e:
        logger.error(f"Erro ao contar registros no PostgreSQL para {table}: {e}")
        return -1


def count_databricks_records(table: str) -> Tuple[bool, int]:
    """
    Verifica se tabela existe no Databricks e conta registros.
    
    Returns:
        (exists: bool, count: int)
    """
    try:
        conn = get_databricks_connection()
        cursor = conn.cursor()
        
        catalog = os.getenv("DATABRICKS_CATALOG", "main")
        schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
        full_table = f"{catalog}.{schema}.{table}"
        
        # Verificar se tabela existe
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return (True, count)
        except Exception as e:
            error_msg = str(e).lower()
            if "table or view not found" in error_msg or "does not exist" in error_msg:
                cursor.close()
                conn.close()
                return (False, 0)
            else:
                raise e
                
    except Exception as e:
        logger.error(f"Erro ao verificar Databricks para {table}: {e}")
        return (False, -1)


def main():
    """Executa comparação entre PostgreSQL e Databricks."""
    print_header("🔍 Comparação PostgreSQL vs Databricks")
    
    print_info("Conectando aos bancos de dados...")
    
    # Verificar conexões
    try:
        pg_conn = get_postgres_connection()
        pg_conn.close()
        print_success("✅ Conexão PostgreSQL OK")
    except Exception as e:
        print_error(f"❌ Erro ao conectar ao PostgreSQL: {e}")
        return 1
    
    try:
        db_conn = get_databricks_connection()
        db_conn.close()
        print_success("✅ Conexão Databricks OK")
    except Exception as e:
        print_error(f"❌ Erro ao conectar ao Databricks: {e}")
        return 1
    
    print_info("\nComparando tabelas...")
    
    # Criar tabela de resultados
    results_table = Table(
        title="📊 Comparação de Tabelas",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan",
    )
    results_table.add_column("Tabela", style="cyan", no_wrap=True)
    results_table.add_column("PostgreSQL", justify="right", style="green")
    results_table.add_column("Databricks", justify="right", style="blue")
    results_table.add_column("Diferença", justify="right", style="yellow")
    results_table.add_column("Status", justify="center")
    
    total_pg = 0
    total_db = 0
    tables_missing = []
    tables_different = []
    
    for table in BRONZE_TABLES:
        # Contar no PostgreSQL
        pg_count = count_postgres_records(table)
        
        # Verificar e contar no Databricks
        db_exists, db_count = count_databricks_records(table)
        
        if not db_exists:
            status = "❌ Não existe"
            tables_missing.append(table)
        elif pg_count == -1 or db_count == -1:
            status = "⚠️ Erro"
        elif pg_count == db_count:
            status = "✅ OK"
        else:
            status = "⚠️ Diferente"
            tables_different.append(table)
        
        # Calcular diferença
        if pg_count >= 0 and db_count >= 0:
            diff = pg_count - db_count
            diff_str = f"{diff:+d}" if diff != 0 else "0"
        else:
            diff_str = "N/A"
        
        # Adicionar linha
        results_table.add_row(
            table,
            str(pg_count) if pg_count >= 0 else "Erro",
            str(db_count) if db_count >= 0 else "N/A" if not db_exists else "Erro",
            diff_str,
            status,
        )
        
        if pg_count >= 0:
            total_pg += pg_count
        if db_count >= 0:
            total_db += db_count
    
    # Adicionar linha de totais
    results_table.add_row(
        "[bold]TOTAL[/bold]",
        f"[bold]{total_pg}[/bold]",
        f"[bold]{total_db}[/bold]",
        f"[bold]{total_pg - total_db:+d}[/bold]",
        "",
        style="bold",
    )
    
    # Exibir tabela
    console = Console()
    console.print()
    console.print(results_table)
    
    # Resumo
    print()
    print_header("📋 Resumo")
    
    if tables_missing:
        print_warning(f"⚠️  {len(tables_missing)} tabela(s) não existe(m) no Databricks:")
        for table in tables_missing:
            print(f"   - {table}")
    
    if tables_different:
        print_warning(f"⚠️  {len(tables_different)} tabela(s) com contagens diferentes:")
        for table in tables_different:
            pg_count = count_postgres_records(table)
            db_exists, db_count = count_databricks_records(table)
            if db_exists:
                print(f"   - {table}: PostgreSQL={pg_count}, Databricks={db_count}")
    
    if not tables_missing and not tables_different:
        print_success("✅ Todas as tabelas existem e têm a mesma quantidade de registros!")
        return 0
    else:
        print_warning("\n💡 Execute 'uv run ingest --table all' para sincronizar os dados.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

