#!/usr/bin/env python3
"""
Gerador de dados para quality_inspections.

Uso:
    python quality_generator.py --mode insert --count 50
    python quality_generator.py --mode update --count 10
    python quality_generator.py --mode upsert --count 20
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
    random_date_mixed_formats,
    maybe_stringify_number,
    random_bool_inconsistent,
    DEFECT_CODES,
    P_STRING_DATE_IN_FIELDS,
    P_STRING_NUMERIC_IN_FIELDS,
    execute_upsert,
    execute_batch_insert,
    RealDictCursor,
    fake,
)


def generate_quality_inspections(count: int, seed: int = None, existing_ids: List[str] = None) -> List[Dict]:
    """
    Gera inspeções de qualidade.
    """
    if seed is not None:
        random.seed(seed)
    
    # Buscar ordens de produção existentes
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT production_order_id, equipment_id FROM bronze.production_orders ORDER BY last_update DESC LIMIT 100")
    prod_orders = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if not prod_orders:
        raise ValueError("Nenhuma ordem de produção encontrada. Execute production_generator.py primeiro.")
    
    data = []
    existing_set = set(existing_ids) if existing_ids else set()
    
    # Encontrar o maior ID numérico existente para começar a partir dele
    max_id_num = 10000
    if existing_ids:
        for existing_id in existing_ids:
            try:
                # Extrair número do ID (ex: "INS010000" -> 10000)
                if existing_id.startswith("INS"):
                    num_part = existing_id[3:]
                    if num_part.isdigit():
                        max_id_num = max(max_id_num, int(num_part))
            except (ValueError, AttributeError):
                pass
    
    for i in range(count):
        # Gerar ID único começando do máximo existente + 1
        start_num = max_id_num + 1 + i
        ins_id = f"INS{start_num:06d}"
        
        # Se ainda houver colisão (improvável, mas seguro), tentar números aleatórios
        attempts = 0
        while ins_id in existing_set and attempts < 100:
            ins_id = f"INS{random.randint(max_id_num + 1, 999999):06d}"
            attempts += 1
        
        if ins_id in existing_set:
            raise ValueError(f"Não foi possível gerar ID único após {attempts} tentativas")
        
        existing_set.add(ins_id)
        
        # Pegar ordem de produção aleatória
        po = random.choice(prod_orders)
        po_id = po[0]
        eq_id = po[1]
        
        total_qty = random.randint(50, 500)
        failed_qty = random.randint(0, int(total_qty * 0.08))  # Até 8% de falhas
        passed = failed_qty == 0
        
        defect_codes = None
        if failed_qty > 0:
            defect_codes = ",".join(random.sample(DEFECT_CODES, random.randint(1, 2)))
        
        inspection_date = datetime.utcnow() - timedelta(hours=random.randint(0, 48))
        
        inspection = {
            "inspection_id": ins_id,
            "production_order_id": po_id,
            "equipment_id": eq_id,
            "inspection_type": random.choice(["visual", "dimensional", "functional"]),
            "inspection_date": (
                random_date_mixed_formats(inspection_date) 
                if random.random() < P_STRING_DATE_IN_FIELDS 
                else inspection_date.strftime("%Y-%m-%d %H:%M:%S")
            ),
            "inspector_id": f"INSP-{random.randint(1,10):03d}",
            "passed": random_bool_inconsistent() if random.random() < 0.3 else str(passed).lower(),
            "failed_quantity": maybe_stringify_number(failed_qty),
            "total_quantity": maybe_stringify_number(total_qty),
            "defect_codes": defect_codes,
            "notes": fake.sentence() if random.random() < 0.5 else None,
            "last_update": random_date_mixed_formats(
                datetime.utcnow() - timedelta(hours=random.randint(0, 6))
            ),
        }
        data.append(inspection)
    
    return data


def generate_update_data(existing_ids: List[str], seed: int = None) -> List[Dict]:
    """
    Gera dados de atualização para inspeções existentes.
    Simula atualizações de resultados, quantidades, etc.
    """
    if seed is not None:
        random.seed(seed)
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    updates = []
    for ins_id in existing_ids:
        cursor.execute(
            "SELECT * FROM bronze.quality_inspections WHERE inspection_id = %s",
            (ins_id,)
        )
        original = cursor.fetchone()
        
        if not original:
            continue
        
        # Simular mudanças: resultados, quantidades
        total_qty = int(original["total_quantity"]) if original["total_quantity"] and original["total_quantity"].isdigit() else 100
        failed_qty = random.randint(0, int(total_qty * 0.08))
        passed = failed_qty == 0
        
        defect_codes = None
        if failed_qty > 0:
            defect_codes = ",".join(random.sample(DEFECT_CODES, random.randint(1, 2)))
        
        update_data = {
            "inspection_id": ins_id,
            "production_order_id": original["production_order_id"],
            "equipment_id": original["equipment_id"],
            "inspection_type": original["inspection_type"],
            "inspection_date": original["inspection_date"],
            "inspector_id": original["inspector_id"],
            "passed": random_bool_inconsistent() if random.random() < 0.3 else str(passed).lower(),
            "failed_quantity": maybe_stringify_number(failed_qty),
            "total_quantity": maybe_stringify_number(total_qty),
            "defect_codes": defect_codes,
            "notes": original["notes"],
            "last_update": random_date_mixed_formats(
                datetime.utcnow() - timedelta(hours=random.randint(0, 6))
            ),
        }
        updates.append(update_data)
    
    cursor.close()
    conn.close()
    return updates


def main():
    parser = argparse.ArgumentParser(description="Gerador de dados para quality_inspections")
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
            # Buscar IDs existentes para evitar colisões
            cursor = conn.cursor()
            cursor.execute("SELECT inspection_id FROM bronze.quality_inspections")
            existing_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            print(f"🔄 Gerando {args.count} novas inspeções de qualidade...")
            data = generate_quality_inspections(args.count, seed=args.seed, existing_ids=existing_ids)
            execute_batch_insert(conn, "bronze.quality_inspections", data, batch_size=args.batch_size)
            print(f"✅ Inseridas {len(data)} inspeções")
            
        elif args.mode == "update":
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT inspection_id FROM bronze.quality_inspections ORDER BY RANDOM() LIMIT {args.count}"
            )
            existing_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            if not existing_ids:
                print("⚠️  Nenhuma inspeção existente encontrada. Use --mode insert primeiro.")
                return
            
            print(f"🔄 Atualizando {len(existing_ids)} inspeções existentes...")
            updates = generate_update_data(existing_ids, seed=args.seed)
            
            for update_data in updates:
                execute_upsert(conn, "bronze.quality_inspections", update_data, "inspection_id", mode="update")
            
            print(f"✅ Atualizadas {len(updates)} inspeções")
            
        elif args.mode == "upsert":
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT inspection_id FROM bronze.quality_inspections ORDER BY RANDOM() LIMIT {args.count // 2}"
            )
            existing_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            print(f"🔄 Gerando {args.count} registros (upsert)...")
            all_data = generate_quality_inspections(
                args.count, 
                seed=args.seed, 
                existing_ids=existing_ids if existing_ids else None
            )
            
            for inspection in all_data:
                execute_upsert(conn, "bronze.quality_inspections", inspection, "inspection_id", mode="upsert")
            
            print(f"✅ Processadas {len(all_data)} inspeções (upsert)")
    
    except ValueError as e:
        print(f"❌ Erro: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

