#!/usr/bin/env python3
import json
import os

# Ler gold_layer.ipynb
with open('notebooks/gold_layer.ipynb', 'r', encoding='utf-8') as f:
    nb = json.load(f)

def extract_code(nb, search_terms):
    """Extrai código de células que contêm os termos de busca"""
    for i, cell in enumerate(nb['cells']):
        source = ''.join(cell.get('source', []))
        if all(term in source for term in search_terms):
            # Procurar próxima célula de código
            for j in range(i + 1, len(nb['cells'])):
                next_cell = nb['cells'][j]
                if next_cell.get('cell_type') == 'code':
                    return ''.join(next_cell.get('source', []))
    return None

def create_dbquery_file(filepath, title, description, code):
    """Cria um arquivo .dbquery.ipynb"""
    if not code:
        print(f'⚠️  Código não encontrado para {filepath}')
        return False
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump({
            "cells": [
                {"cell_type": "markdown", "metadata": {}, "source": [f"# {title}\n\n{description}"]},
                {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": code.splitlines(keepends=True)}
            ],
            "metadata": {"language_info": {"name": "python"}},
            "nbformat": 4,
            "nbformat_minor": 2
        }, f, indent=1, ensure_ascii=False)
    return True

models_gold = 'models/gold'
os.makedirs(models_gold, exist_ok=True)

# Dimensões
files = [
    ('dim_tempo.dbquery.ipynb', 'Dimensão Tempo', 'Criação e carga da tabela de dimensão de tempo (calendário).', ['dim_tempo', 'CREATE TABLE']),
    ('dim_equipamento.dbquery.ipynb', 'Dimensão Equipamento (Snapshot Atual)', 'Visão SCD Type 1 (Current Snapshot) baseada na Silver.', ['dim_equipamento', 'equipment_clean']),
    ('dim_equipamento_scd.dbquery.ipynb', 'Dimensão Equipamento SCD (Histórico)', 'Visão SCD Type 2 (Historical) baseada na Silver.', ['dim_equipamento_scd', 'equipment_scd']),
    ('dim_produto.dbquery.ipynb', 'Dimensão Produto', 'Dimensão de produtos fabricados.', ['dim_produto', 'production_orders_clean']),
    ('dim_tecnico.dbquery.ipynb', 'Dimensão Técnico', 'Dimensão de técnicos de manutenção.', ['dim_tecnico', 'maintenance_orders_clean']),
    ('dim_tipo_manutencao.dbquery.ipynb', 'Dimensão Tipo Manutenção', 'Dimensão de tipos de manutenção.', ['dim_tipo_manutencao']),
    ('dim_defeito.dbquery.ipynb', 'Dimensão Defeito', 'Dimensão de códigos de defeito.', ['dim_defeito', 'quality_inspections_clean']),
    ('fact_producao.dbquery.ipynb', 'Fact Produção', 'Tabela fato de produção com métricas de quantidade e duração.', ['fact_producao', 'production_orders_clean']),
    ('fact_manutencao.dbquery.ipynb', 'Fact Manutenção', 'Tabela fato de manutenção com cálculo de downtime.', ['fact_manutencao', 'maintenance_orders_clean']),
    ('fact_qualidade.dbquery.ipynb', 'Fact Qualidade', 'Tabela fato de inspeções de qualidade e taxas de rejeição.', ['fact_qualidade', 'quality_inspections_clean']),
    ('fact_iot_agregado.dbquery.ipynb', 'Fact IoT Agregado (Horário)', 'Agregação horária de leituras de sensores para otimização de queries.', ['fact_iot_agregado', 'iot_readings_clean']),
    ('vw_oee_diario.dbquery.ipynb', 'View OEE Diário', 'Cálculo de OEE (Overall Equipment Effectiveness) diário.\nOEE = Disponibilidade x Performance x Qualidade', ['vw_oee_diario']),
    ('vw_downtime_por_causa.dbquery.ipynb', 'View Downtime por Causa', 'Downtime agregado por tipo de manutenção e equipamento.', ['vw_downtime_por_causa']),
    ('vw_equipamentos_criticos.dbquery.ipynb', 'View Equipamentos Críticos', 'Equipamentos com maior downtime ou menor OEE.', ['vw_equipamentos_criticos']),
    ('vw_tendencias_sensores.dbquery.ipynb', 'View Tendências Sensores', 'Tendências de leituras de sensores por equipamento e tipo.', ['vw_tendencias_sensores']),
]

for filename, title, desc, search_terms in files:
    code = extract_code(nb, search_terms)
    
    # Ajustes específicos
    if filename == 'fact_iot_agregado.dbquery.ipynb' and not code:
        code = extract_code(nb, ['fact_iot_hourly', 'iot_sensor_readings'])
        if code:
            code = code.replace('fact_iot_hourly', 'fact_iot_agregado')
            code = code.replace('iot_sensor_readings', 'iot_readings_clean')
    
    if filename == 'vw_oee_diario.dbquery.ipynb' and not code:
        code = extract_code(nb, ['vw_oee_daily'])
        if code:
            code = code.replace('vw_oee_daily', 'vw_oee_diario')
            # Corrigir join para usar dim_equipamento
            code = code.replace(
                "dim_equipamento_scd')} e ON p.equipment_id = e.equipment_id AND e.is_current = true",
                "dim_equipamento')} e ON p.equipment_id = e.equipment_id"
            )
    
    if create_dbquery_file(f'{models_gold}/{filename}', title, desc, code):
        print(f'✅ {filename}')
    else:
        print(f'⚠️  {filename} - código não encontrado')

# Remover arquivos antigos
old_files = ['fact_iot_hourly.dbquery.ipynb', 'vw_oee_daily.dbquery.ipynb']
for old_file in old_files:
    old_path = f'{models_gold}/{old_file}'
    if os.path.exists(old_path):
        os.remove(old_path)
        print(f'✅ Removido {old_file} (antigo)')

print('\n✅ Todos os arquivos .dbquery.ipynb foram atualizados/criados!')

