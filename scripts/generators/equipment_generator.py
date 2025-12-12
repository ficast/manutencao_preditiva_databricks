#!/usr/bin/env python3
"""
Gerador de dados para equipment_master.

Uso:
    python equipment_generator.py --mode insert --count 50
    python equipment_generator.py --mode update --count 10
    python equipment_generator.py --mode upsert --count 20
"""

import argparse
import random
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict

# Adicionar diretório scripts ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.common import (
    get_db_connection,
    random_status_inconsistent,
    random_date_between,
    random_date_mixed_formats,
    alnum,
    EQUIPMENT_STATUSES,
    EQUIPMENT_TYPES,
    MANUFACTURERS,
    LOCATIONS,
    P_STRING_DATE_IN_FIELDS,
    P_NULL_LOCATION,
    execute_upsert,
    execute_batch_insert,
    RealDictCursor,
)
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


def generate_equipment_data(count: int, seed: int = None, existing_ids: List[str] = None) -> List[Dict]:
    """
    Gera dados de equipamentos.
    
    Args:
        count: Quantidade de registros a gerar
        seed: Seed para reprodutibilidade
        existing_ids: Lista de IDs existentes (para updates)
    
    Returns:
        Lista de dicionários com dados dos equipamentos
    """
    if seed is not None:
        random.seed(seed)
    
    data = []
    existing_set = set(existing_ids) if existing_ids else set()
    
    # Encontrar o maior ID numérico existente para começar a partir dele
    max_id_num = 10000
    if existing_ids:
        for existing_id in existing_ids:
            try:
                # Extrair número do ID (ex: "EQ10000" -> 10000)
                if existing_id.startswith("EQ"):
                    num_part = existing_id[2:]
                    if num_part.isdigit():
                        max_id_num = max(max_id_num, int(num_part))
            except (ValueError, AttributeError):
                pass
    
    for i in range(count):
        # Gerar ID único começando do máximo existente + 1
        if existing_ids and i < len(existing_ids):
            eid = existing_ids[i]
        else:
            start_num = max_id_num + 1 + i
            eid = f"EQ{start_num:05d}"
            
            # Se ainda houver colisão (improvável, mas seguro), tentar números aleatórios
            attempts = 0
            while eid in existing_set and attempts < 100:
                eid = f"EQ{random.randint(max_id_num + 1, 99999):05d}"
                attempts += 1
            
            if eid in existing_set:
                raise ValueError(f"Não foi possível gerar ID único após {attempts} tentativas")
            
            existing_set.add(eid)
        
        eq_type = random.choice(EQUIPMENT_TYPES)
        name = f"{eq_type}-{random.choice(['Alpha','Beta','Gamma','Delta'])}-{alnum(4)}"
        location = None if random.random() < P_NULL_LOCATION else random.choice(LOCATIONS)
        install_date = random_date_between(2000)
        
        equipment = {
            "equipment_id": eid,
            "equipment_name": name,
            "equipment_type": eq_type,
            "location": location,
            "installation_date": (
                random_date_mixed_formats(install_date) 
                if random.random() < P_STRING_DATE_IN_FIELDS 
                else install_date.strftime("%Y-%m-%d")
            ),
            "manufacturer": random.choice(MANUFACTURERS),
            "model": f"MOD-{random.randint(100,999)}",
            "status": random_status_inconsistent(EQUIPMENT_STATUSES),
            "last_update_date": random_date_mixed_formats(
                datetime.utcnow() - timedelta(hours=random.randint(0, 24))
            ),
        }
        data.append(equipment)
    
    return data


def generate_update_data(existing_ids: List[str], seed: int = None) -> List[Dict]:
    """
    Gera dados de atualização para equipamentos existentes.
    Simula mudanças de status, localização, etc.
    
    Args:
        existing_ids: Lista de IDs de equipamentos existentes
        seed: Seed para reprodutibilidade
    
    Returns:
        Lista de dicionários com dados atualizados
    """
    if seed is not None:
        random.seed(seed)
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    updates = []
    for eid in existing_ids:
        # Buscar dados originais
        cursor.execute(
            "SELECT * FROM bronze.equipment_master WHERE equipment_id = %s",
            (eid,)
        )
        original = cursor.fetchone()
        
        if not original:
            continue
        
        # Simular mudanças: status, localização ou ambos
        new_status = random_status_inconsistent(EQUIPMENT_STATUSES)
        new_location = random.choice(LOCATIONS) if random.random() > 0.3 else None
        
        update_data = {
            "equipment_id": eid,
            "equipment_name": original["equipment_name"],
            "equipment_type": original["equipment_type"],
            "location": new_location if new_location else original["location"],
            "installation_date": original["installation_date"],
            "manufacturer": original["manufacturer"],
            "model": original["model"],
            "status": new_status,  # Mudança de status
            "last_update_date": random_date_mixed_formats(
                datetime.utcnow() - timedelta(hours=random.randint(0, 6))
            ),
        }
        updates.append(update_data)
    
    cursor.close()
    conn.close()
    return updates


def main():
    parser = argparse.ArgumentParser(description="Gerador de dados para equipment_master")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["insert", "update", "upsert"],
        required=True,
        help="Modo de operação: insert (novos), update (atualiza existentes), upsert (insert ou update)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=50,
        help="Quantidade de registros (para insert/upsert) ou IDs para atualizar (para update)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Seed para reprodutibilidade"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Tamanho do lote para inserções em massa"
    )
    
    args = parser.parse_args()
    
    # Header
    print_header(
        "🔧 Equipment Master Generator",
        f"Modo: {args.mode.upper()} | Quantidade: {args.count} | Seed: {args.seed or 'aleatório'}"
    )
    
    start_time = datetime.now()
    conn = get_db_connection()
    
    try:
        if args.mode == "insert":
            # Buscar IDs existentes para evitar colisões
            cursor = conn.cursor()
            cursor.execute("SELECT equipment_id FROM bronze.equipment_master")
            existing_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Gerando {args.count} novos equipamentos...")
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            ) as progress:
                task = progress.add_task("Gerando dados...", total=args.count)
                data = generate_equipment_data(args.count, seed=args.seed, existing_ids=existing_ids)
                progress.update(task, completed=args.count)
            
            logger.info(f"Inserindo {len(data)} registros no PostgreSQL...")
            execute_batch_insert(conn, "bronze.equipment_master", data, batch_size=args.batch_size)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            print_success(f"Inseridos {len(data)} equipamentos em {elapsed:.2f}s")
            
            # Resumo
            print_summary_table({
                "Modo": args.mode,
                "Registros gerados": len(data),
                "Tempo total": f"{elapsed:.2f}s",
                "Taxa": f"{len(data)/elapsed:.1f} registros/s" if elapsed > 0 else "N/A"
            })
            
        elif args.mode == "update":
            logger.info(f"Buscando {args.count} equipamentos existentes para atualizar...")
            
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT equipment_id FROM bronze.equipment_master ORDER BY RANDOM() LIMIT {args.count}"
            )
            existing_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            if not existing_ids:
                print_warning("Nenhum equipamento existente encontrado. Use --mode insert primeiro.")
                return
            
            logger.info(f"Gerando dados de atualização para {len(existing_ids)} equipamentos...")
            updates = generate_update_data(existing_ids, seed=args.seed)
            
            logger.info(f"Atualizando {len(updates)} registros...")
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            ) as progress:
                task = progress.add_task("Atualizando...", total=len(updates))
                for i, update_data in enumerate(updates):
                    execute_upsert(conn, "bronze.equipment_master", update_data, "equipment_id", mode="update")
                    progress.update(task, advance=1)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            print_success(f"Atualizados {len(updates)} equipamentos em {elapsed:.2f}s")
            
            print_summary_table({
                "Modo": args.mode,
                "Registros atualizados": len(updates),
                "Tempo total": f"{elapsed:.2f}s",
            })
            
        elif args.mode == "upsert":
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT equipment_id FROM bronze.equipment_master ORDER BY RANDOM() LIMIT {args.count // 2}"
            )
            existing_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            inserts_count = args.count - len(existing_ids)
            updates_count = len(existing_ids)
            
            logger.info(f"Gerando {args.count} registros (upsert: {updates_count} updates + {inserts_count} inserts)...")
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            ) as progress:
                task = progress.add_task("Gerando dados...", total=args.count)
                all_data = generate_equipment_data(
                    args.count, 
                    seed=args.seed, 
                    existing_ids=existing_ids if existing_ids else None
                )
                progress.update(task, completed=args.count)
            
            logger.info(f"Processando {len(all_data)} registros (upsert)...")
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            ) as progress:
                task = progress.add_task("Processando...", total=len(all_data))
                for equipment in all_data:
                    execute_upsert(conn, "bronze.equipment_master", equipment, "equipment_id", mode="upsert")
                    progress.update(task, advance=1)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            print_success(f"Processados {len(all_data)} equipamentos (upsert) em {elapsed:.2f}s")
            
            print_summary_table({
                "Modo": args.mode,
                "Total processados": len(all_data),
                "Inserts": inserts_count,
                "Updates": updates_count,
                "Tempo total": f"{elapsed:.2f}s",
            })
    
    except Exception as e:
        logger.error(f"Erro durante execução: {e}", exc_info=True)
        print_error(f"Falha: {e}")
        raise
    
    finally:
        conn.close()
        logger.info("Conexão com PostgreSQL fechada")


if __name__ == "__main__":
    main()

