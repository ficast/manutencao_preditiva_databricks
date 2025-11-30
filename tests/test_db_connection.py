from scripts.generators.common import get_db_connection


def test_db_connection():
    """Testa conexão com PostgreSQL e verifica schema e tabelas."""
    print("🔌 Conectando ao PostgreSQL...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Verificar se schema bronze existe
    cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'bronze'")
    schemas = cursor.fetchall()
    assert len(schemas) > 0, "Schema 'bronze' não encontrado"
    print(f"✅ Schema 'bronze' encontrado")
    
    # Listar tabelas no schema bronze
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        ORDER BY table_name
    """)
    tables = cursor.fetchall()
    
    assert len(tables) > 0, "Nenhuma tabela encontrada no schema 'bronze'"
    print(f"\n📊 Tabelas encontradas no schema 'bronze': {len(tables)}")
    
    # Verificar tabelas esperadas
    expected_tables = {
        "equipment_master",
        "iot_sensor_readings",
        "production_orders",
        "maintenance_orders",
        "quality_inspections"
    }
    found_tables = {table[0] for table in tables}
    
    for table_name in found_tables:
        cursor.execute(f"SELECT COUNT(*) FROM bronze.{table_name}")
        count = cursor.fetchone()[0]
        print(f"   - {table_name}: {count} registros")
    
    # Verificar se todas as tabelas esperadas existem
    missing_tables = expected_tables - found_tables
    assert len(missing_tables) == 0, f"Tabelas faltando: {missing_tables}"
    
    cursor.close()
    conn.close()
    
    print("\n✅ Conexão bem-sucedida e todas as tabelas encontradas!")
