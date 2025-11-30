#!/usr/bin/env python3
"""
Gerador de dados para production_orders.

Uso:
    python production_generator.py --mode insert --count 50
    python production_generator.py --mode update --count 10
    python production_generator.py --mode upsert --count 20
"""

import argparse
import random
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# Adicionar diretório scripts ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.common import (
    get_db_connection,
    random_status_inconsistent,
    random_date_mixed_formats,
    maybe_stringify_number,
    PRODUCTION_STATUSES,
    P_STRING_DATE_IN_FIELDS,
    P_STRING_NUMERIC_IN_FIELDS,
    execute_upsert,
    execute_batch_insert,
    RealDictCursor,
)


def generate_production_orders(count: int, seed: int = None, existing_ids: List[str] = None) -> List[Dict]:
    """
    Gera ordens de produção.
    
    Args:
        count: Quantidade de ordens a gerar
        seed: Seed para reprodutibilidade
        existing_ids: Lista de IDs existentes (para updates)
    
    Returns:
        Lista de dicionários com dados das ordens
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
            po_id = existing_ids[i]
        else:
            po_id = f"PO{10000 + i:06d}"
            while po_id in existing_set:
                po_id = f"PO{10000 + random.randint(10000, 99999):06d}"
            existing_set.add(po_id)
        
        eq_id = random.choice(equipment_ids)
        product_id = f"PROD-{random.randint(100,999)}"
        
        # Ordens recentes (últimos 7 dias)
        planned_start = datetime.utcnow() - timedelta(days=random.randint(0, 7))
        planned_end = planned_start + timedelta(hours=random.randint(2, 8))
        actual_start = planned_start + timedelta(minutes=random.randint(-30, 60))
        actual_end = actual_start + timedelta(hours=random.randint(2, 8))
        
        planned_qty = random.randint(100, 1000)
        actual_qty = int(planned_qty * random.uniform(0.85, 1.05))  # Eficiência 85-105%
        
        status = random.choice(PRODUCTION_STATUSES)
        
        order = {
            "production_order_id": po_id,
            "equipment_id": eq_id,
            "product_id": product_id,
            "planned_start": (
                random_date_mixed_formats(planned_start) 
                if random.random() < P_STRING_DATE_IN_FIELDS 
                else planned_start.strftime("%Y-%m-%d %H:%M:%S")
            ),
            "planned_end": (
                random_date_mixed_formats(planned_end) 
                if random.random() < P_STRING_DATE_IN_FIELDS 
                else planned_end.strftime("%Y-%m-%d %H:%M:%S")
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
            "planned_quantity": maybe_stringify_number(planned_qty),
            "actual_quantity": maybe_stringify_number(actual_qty),
            "status": random_status_inconsistent(PRODUCTION_STATUSES),
            "last_update": random_date_mixed_formats(
                datetime.utcnow() - timedelta(hours=random.randint(0, 6))
            ),
        }
        data.append(order)
    
    return data


def generate_update_data(existing_ids: List[str], seed: int = None) -> List[Dict]:
    """
    Gera dados de atualização para ordens existentes.
    Simula mudanças de status, quantidades, etc.
    """
    if seed is not None:
        random.seed(seed)
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    updates = []
    for po_id in existing_ids:
        cursor.execute(
            "SELECT * FROM bronze.production_orders WHERE production_order_id = %s",
            (po_id,)
        )
        original = cursor.fetchone()
        
        if not original:
            continue
        
        # Simular mudanças: status, quantidades
        new_status = random_status_inconsistent(PRODUCTION_STATUSES)
        
        # Atualizar quantidades (pode ter mudado)
        planned_qty = int(original["planned_quantity"]) if original["planned_quantity"].isdigit() else 100
        actual_qty = int(planned_qty * random.uniform(0.85, 1.05))
        
        update_data = {
            "production_order_id": po_id,
            "equipment_id": original["equipment_id"],
            "product_id": original["product_id"],
            "planned_start": original["planned_start"],
            "planned_end": original["planned_end"],
            "actual_start": original["actual_start"],
            "actual_end": original["actual_end"],
            "planned_quantity": maybe_stringify_number(planned_qty),
            "actual_quantity": maybe_stringify_number(actual_qty),
            "status": new_status,  # Mudança de status
            "last_update": random_date_mixed_formats(
                datetime.utcnow() - timedelta(hours=random.randint(0, 6))
            ),
        }
        updates.append(update_data)
    
    cursor.close()
    conn.close()
    return updates


def main():
    parser = argparse.ArgumentParser(description="Gerador de dados para production_orders")
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
            print(f"🔄 Gerando {args.count} novas ordens de produção...")
            data = generate_production_orders(args.count, seed=args.seed)
            execute_batch_insert(conn, "bronze.production_orders", data, batch_size=args.batch_size)
            print(f"✅ Inseridas {len(data)} ordens de produção")
            
        elif args.mode == "update":
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT production_order_id FROM bronze.production_orders ORDER BY RANDOM() LIMIT {args.count}"
            )
            existing_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            if not existing_ids:
                print("⚠️  Nenhuma ordem existente encontrada. Use --mode insert primeiro.")
                return
            
            print(f"🔄 Atualizando {len(existing_ids)} ordens existentes...")
            updates = generate_update_data(existing_ids, seed=args.seed)
            
            for update_data in updates:
                execute_upsert(conn, "bronze.production_orders", update_data, "production_order_id", mode="update")
            
            print(f"✅ Atualizadas {len(updates)} ordens")
            
        elif args.mode == "upsert":
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT production_order_id FROM bronze.production_orders ORDER BY RANDOM() LIMIT {args.count // 2}"
            )
            existing_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            print(f"🔄 Gerando {args.count} registros (upsert)...")
            all_data = generate_production_orders(
                args.count, 
                seed=args.seed, 
                existing_ids=existing_ids if existing_ids else None
            )
            
            for order in all_data:
                execute_upsert(conn, "bronze.production_orders", order, "production_order_id", mode="upsert")
            
            print(f"✅ Processadas {len(all_data)} ordens (upsert)")
    
    except ValueError as e:
        print(f"❌ Erro: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

