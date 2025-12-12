#!/usr/bin/env python3
"""
Gerador de dados para iot_sensor_readings (append-only).

Uso:
    python iot_generator.py --count 1000
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
    random_date_mixed_formats,
    SENSOR_TYPES,
    execute_batch_insert,
)


# Ranges por tipo de sensor
SENSOR_RANGES = {
    "temperature": (20, 100),  # Celsius
    "vibration": (0, 50),      # mm/s
    "pressure": (0, 10),        # bar
    "humidity": (30, 90),       # %
    "current": (0, 100)         # Amperes
}

UNITS_MAP = {
    "temperature": "°C",
    "vibration": "mm/s",
    "pressure": "bar",
    "humidity": "%",
    "current": "A"
}


def generate_iot_readings(count: int, seed: int = None, hours_back: int = 24, existing_ids: List[str] = None) -> List[Dict]:
    """
    Gera leituras IoT (append-only).
    
    Args:
        count: Quantidade de leituras a gerar
        seed: Seed para reprodutibilidade
        hours_back: Quantas horas atrás começar a gerar timestamps
        existing_ids: Lista de IDs existentes para evitar colisões
    
    Returns:
        Lista de dicionários com leituras IoT
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
    
    readings = []
    existing_set = set(existing_ids) if existing_ids else set()
    
    # Encontrar o maior ID numérico existente para começar a partir dele
    max_id_num = 1000000
    if existing_ids:
        for existing_id in existing_ids:
            try:
                # Extrair número do ID (ex: "IOT01000000" -> 1000000)
                if existing_id.startswith("IOT"):
                    num_part = existing_id[3:]
                    if num_part.isdigit():
                        max_id_num = max(max_id_num, int(num_part))
            except (ValueError, AttributeError):
                pass
    
    for i in range(count):
        # Gerar ID único começando do máximo existente + 1
        start_num = max_id_num + 1 + i
        reading_id = f"IOT{start_num:08d}"
        
        # Se ainda houver colisão (improvável, mas seguro), tentar números aleatórios
        attempts = 0
        while reading_id in existing_set and attempts < 100:
            reading_id = f"IOT{random.randint(max_id_num + 1, 99999999):08d}"
            attempts += 1
        
        if reading_id in existing_set:
            raise ValueError(f"Não foi possível gerar ID único após {attempts} tentativas")
        
        existing_set.add(reading_id)
        
        eq_id = random.choice(equipment_ids)
        sensor_type = random.choice(SENSOR_TYPES)
        
        # Variação no nome do tipo (simular inconsistências)
        if random.random() < 0.20:
            sensor_type_display = random.choice([
                sensor_type.upper(),
                sensor_type.capitalize(),
                sensor_type.replace("_", " ")
            ])
        else:
            sensor_type_display = sensor_type
        
        sensor_id = f"SENS-{eq_id}-{sensor_type[:3].upper()}-{random.randint(1,5)}"
        
        # Gerar valor dentro do range
        min_val, max_val = SENSOR_RANGES[sensor_type]
        base_value = random.uniform(min_val, max_val)
        
        # Valores fora de range (anomalias) - 2% das vezes
        if random.random() < 0.02:
            if random.random() < 0.5:
                value = base_value * random.uniform(1.5, 3.0)  # acima do range
            else:
                value = base_value * random.uniform(-0.5, 0.3)  # abaixo do range
        else:
            value = base_value
        
        # Timestamp das últimas N horas
        timestamp = datetime.utcnow() - timedelta(
            hours=random.randint(0, hours_back),
            minutes=random.randint(0, 59)
        )
        reading_ts = (
            random_date_mixed_formats(timestamp) 
            if random.random() < 0.3 
            else timestamp.strftime("%Y-%m-%d %H:%M:%S")
        )
        
        reading = {
            "reading_id": reading_id,
            "equipment_id": eq_id,
            "sensor_id": sensor_id,
            "sensor_type": sensor_type_display,
            "reading_value": str(round(value, 2)),
            "reading_timestamp": reading_ts,
            "unit": UNITS_MAP[sensor_type],
        }
        readings.append(reading)
    
    return readings


def main():
    parser = argparse.ArgumentParser(description="Gerador de dados para iot_sensor_readings (append-only)")
    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="Quantidade de leituras a gerar"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Seed para reprodutibilidade"
    )
    parser.add_argument(
        "--hours-back",
        type=int,
        default=24,
        help="Quantas horas atrás começar a gerar timestamps"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Tamanho do lote para inserções em massa"
    )
    
    args = parser.parse_args()
    
    print(f"🔄 Gerando {args.count} leituras IoT (últimas {args.hours_back}h)...")
    
    try:
        # Buscar IDs existentes para evitar colisões
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT reading_id FROM bronze.iot_sensor_readings")
        existing_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        readings = generate_iot_readings(args.count, seed=args.seed, hours_back=args.hours_back, existing_ids=existing_ids)
        
        conn = get_db_connection()
        execute_batch_insert(conn, "bronze.iot_sensor_readings", readings, batch_size=args.batch_size)
        conn.close()
        
        print(f"✅ Inseridas {len(readings)} leituras IoT")
        
    except ValueError as e:
        print(f"❌ Erro: {e}")


if __name__ == "__main__":
    main()

