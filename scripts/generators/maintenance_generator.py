#!/usr/bin/env python3
"""
Gerador de dados para maintenance_orders.

Uso:
    python maintenance_generator.py --mode insert --count 50
    python maintenance_generator.py --mode update --count 10
    python maintenance_generator.py --mode upsert --count 20
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
    random_date_mixed_formats,
    maybe_stringify_number,
    MAINTENANCE_STATUSES,
    MAINTENANCE_TYPES,
    MAINTENANCE_PRIORITIES,
    P_STRING_DATE_IN_FIELDS,
    execute_upsert,
    execute_batch_insert,
    RealDictCursor,
    fake,
)


def generate_maintenance_orders(count: int, seed: int = None, existing_ids: List[str] = None) -> List[Dict]:
    """
    Gera ordens de manutenção.
    """
    if seed is not None:
        random.seed(seed)
    
    # Buscar equipamentos existentes
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT equipment_id FROM bronze.equipment_master")
    equipment_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    
    if not equipment_ids:
        raise ValueError("Nenhum equipamento encontrado. Execute equipment_generator.py primeiro.")
    
    data = []
    existing_set = set(existing_ids) if existing_ids else set()
    
    for i in range(count):
        # Gerar ID único
        if existing_ids and i < len(existing_ids):
            mo_id = existing_ids[i]
        else:
            mo_id = f"MO{10000 + i:06d}"
            while mo_id in existing_set:
                mo_id = f"MO{10000 + random.randint(10000, 99999):06d}"
            existing_set.add(mo_id)
        
        eq_id = random.choice(equipment_ids)
        maintenance_type = random.choice(MAINTENANCE_TYPES)
        
        scheduled_start = datetime.utcnow() - timedelta(days=random.randint(0, 3))
        scheduled_end = scheduled_start + timedelta(hours=random.randint(1, 4))
        actual_start = scheduled_start + timedelta(minutes=random.randint(-15, 30))
        actual_end = actual_start + timedelta(hours=random.randint(1, 4))
        
        order = {
            "maintenance_order_id": mo_id,
            "equipment_id": eq_id,
            "maintenance_type": random_status_inconsistent([maintenance_type]),
            "scheduled_start": (
                random_date_mixed_formats(scheduled_start) 
                if random.random() < P_STRING_DATE_IN_FIELDS 
                else scheduled_start.strftime("%Y-%m-%d %H:%M:%S")
            ),
            "scheduled_end": (
                random_date_mixed_formats(scheduled_end) 
                if random.random() < P_STRING_DATE_IN_FIELDS 
                else scheduled_end.strftime("%Y-%m-%d %H:%M:%S")
            ),
            "actual_start": (
                random_date_mixed_formats(actual_start) 
                if random.random() < P_STRING_DATE_IN_FIELDS 
                else actual_start.strftime("%Y-%m-%d %H:%M:%S")
            ),
            "actual_end": (
                random_date_mixed_formats(actual_end) 
                if random.random() < P_STRING_DATE_IN_FIELDS 
                else actual_end.strftime("%Y-%m-%d %H:%M:%S")
            ),
            "technician_id": f"TECH-{random.randint(1,20):03d}",
            "status": random_status_inconsistent(MAINTENANCE_STATUSES),
            "priority": random.choice(MAINTENANCE_PRIORITIES),
            "description": f"Manutenção {maintenance_type} - {fake.sentence()}",
            "last_update": random_date_mixed_formats(
                datetime.utcnow() - timedelta(hours=random.randint(0, 6))
            ),
        }
        data.append(order)
    
    return data


def generate_update_data(existing_ids: List[str], seed: int = None) -> List[Dict]:
    """
    Gera dados de atualização para ordens de manutenção existentes.
    """
    if seed is not None:
        random.seed(seed)
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    updates = []
    for mo_id in existing_ids:
        cursor.execute(
            "SELECT * FROM bronze.maintenance_orders WHERE maintenance_order_id = %s",
            (mo_id,)
        )
        original = cursor.fetchone()
        
        if not original:
            continue
        
        # Simular mudanças: status, datas reais
        new_status = random_status_inconsistent(MAINTENANCE_STATUSES)
        
        # Atualizar datas reais (se ainda não foram preenchidas)
        if original["actual_start"] is None or original["actual_start"] == "":
            scheduled_start = datetime.strptime(original["scheduled_start"], "%Y-%m-%d %H:%M:%S")
            actual_start = scheduled_start + timedelta(minutes=random.randint(-15, 30))
            actual_end = actual_start + timedelta(hours=random.randint(1, 4))
        else:
            actual_start = original["actual_start"]
            actual_end = original["actual_end"]
        
        update_data = {
            "maintenance_order_id": mo_id,
            "equipment_id": original["equipment_id"],
            "maintenance_type": original["maintenance_type"],
            "scheduled_start": original["scheduled_start"],
            "scheduled_end": original["scheduled_end"],
            "actual_start": (
                random_date_mixed_formats(actual_start) 
                if isinstance(actual_start, datetime)
                else actual_start
            ),
            "actual_end": (
                random_date_mixed_formats(actual_end) 
                if isinstance(actual_end, datetime)
                else actual_end
            ),
            "technician_id": original["technician_id"],
            "status": new_status,  # Mudança de status
            "priority": original["priority"],
            "description": original["description"],
            "last_update": random_date_mixed_formats(
                datetime.utcnow() - timedelta(hours=random.randint(0, 6))
            ),
        }
        updates.append(update_data)
    
    cursor.close()
    conn.close()
    return updates


def main():
    parser = argparse.ArgumentParser(description="Gerador de dados para maintenance_orders")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["insert", "update", "upsert"],
        required=True,
        help="Modo de operação"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=50,
        help="Quantidade de registros"
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
    
    conn = get_db_connection()
    
    try:
        if args.mode == "insert":
            print(f"🔄 Gerando {args.count} novas ordens de manutenção...")
            data = generate_maintenance_orders(args.count, seed=args.seed)
            execute_batch_insert(conn, "bronze.maintenance_orders", data, batch_size=args.batch_size)
            print(f"✅ Inseridas {len(data)} ordens de manutenção")
            
        elif args.mode == "update":
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT maintenance_order_id FROM bronze.maintenance_orders ORDER BY RANDOM() LIMIT {args.count}"
            )
            existing_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            if not existing_ids:
                print("⚠️  Nenhuma ordem existente encontrada. Use --mode insert primeiro.")
                return
            
            print(f"🔄 Atualizando {len(existing_ids)} ordens existentes...")
            updates = generate_update_data(existing_ids, seed=args.seed)
            
            for update_data in updates:
                execute_upsert(conn, "bronze.maintenance_orders", update_data, "maintenance_order_id", mode="update")
            
            print(f"✅ Atualizadas {len(updates)} ordens")
            
        elif args.mode == "upsert":
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT maintenance_order_id FROM bronze.maintenance_orders ORDER BY RANDOM() LIMIT {args.count // 2}"
            )
            existing_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            print(f"🔄 Gerando {args.count} registros (upsert)...")
            all_data = generate_maintenance_orders(
                args.count, 
                seed=args.seed, 
                existing_ids=existing_ids if existing_ids else None
            )
            
            for order in all_data:
                execute_upsert(conn, "bronze.maintenance_orders", order, "maintenance_order_id", mode="upsert")
            
            print(f"✅ Processadas {len(all_data)} ordens (upsert)")
    
    except ValueError as e:
        print(f"❌ Erro: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

