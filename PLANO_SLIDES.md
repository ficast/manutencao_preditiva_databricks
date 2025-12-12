# Apresentação: Lakehouse para Manufatura e Manutenção Preditiva

---

## SLIDE 1: Introdução - O Case

### Desafio do Negócio
**Problema**: Indústria de manufatura enfrenta:
- **Downtime não planejado** causando perdas de produção
- **Falta de visibilidade** sobre eficiência dos equipamentos (OEE)
- **Dados fragmentados** em múltiplos sistemas (ERP, IoT, Qualidade)
- **Manutenção reativa** em vez de preditiva

### Objetivo do Projeto
Construir um **Lakehouse moderno** no Databricks para:
- ✅ **Integrar dados heterogêneos** (IoT, ERP, Qualidade)
- ✅ **Calcular métricas de OEE** (Overall Equipment Effectiveness)
- ✅ **Identificar equipamentos críticos** e padrões de falha
- ✅ **Habilitar manutenção preditiva** através de análise de tendências
- ✅ **Reduzir downtime** e otimizar eficiência operacional

### Impacto Esperado
- 📊 **Visibilidade em tempo real** do desempenho dos equipamentos
- 🔧 **Manutenção proativa** baseada em dados
- 📈 **Aumento do OEE** através de insights acionáveis
- 💰 **Redução de custos** com paradas não planejadas

---

## SLIDE 2: Gestão de Dados Moderna

### Databricks Lakehouse Platform
**O que é**: Plataforma unificada que combina:
- **Data Lake** (armazenamento escalável e econômico)
- **Data Warehouse** (consultas SQL de alto desempenho)
- **Data Science & ML** (análise avançada e machine learning)

**Por que Databricks?**
- 🚀 **Performance**: Processamento distribuído com Apache Spark
- 🔒 **Governança**: Unity Catalog para gestão centralizada de dados
- 💾 **Delta Lake**: Tabelas ACID, versionamento e time travel
- 🔄 **Integração**: Conectores para múltiplas fontes de dados
- ☁️ **Escalabilidade**: Infraestrutura cloud-native

### Medallion Architecture
**Padrão de arquitetura em camadas** para organização de dados:

```
┌─────────────────────────────────────────┐
│         BRONZE (Raw Data)                │
│  • Dados brutos, sem transformação      │
│  • Preserva formato original            │
│  • Append-only (histórico completo)     │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│         SILVER (Cleaned Data)           │
│  • Dados curados e validados           │
│  • Deduplicação e normalização          │
│  • SCD Type 2 para histórico            │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│         GOLD (Business Layer)            │
│  • Modelo dimensional (Star Schema)     │
│  • Otimizado para análise              │
│  • Views analíticas prontas             │
└─────────────────────────────────────────┘
```

**Benefícios**:
- 📦 **Separação de responsabilidades**: cada camada tem propósito claro
- 🔄 **Processamento incremental**: apenas novos dados são processados
- 🛡️ **Idempotência**: reprocessamento seguro sem duplicações
- 📊 **Performance**: Gold otimizado para consultas analíticas

---

## SLIDE 3: Camada Bronze - Ingestão de Dados Brutos

### O que foi feito
**Objetivo**: Ingerir dados de múltiplas fontes mantendo formato original

### Fontes de Dados
1. **PostgreSQL Local** (simulação de sistemas legados)
   - `equipment_master`: Cadastro de equipamentos
   - `production_orders`: Ordens de produção do ERP
   - `maintenance_orders`: Ordens de manutenção
   - `quality_inspections`: Inspeções de qualidade
   - `iot_sensor_readings`: Leituras de sensores IoT

### Implementação
- ✅ **Scripts Python** para geração de dados sintéticos (`scripts/generators/`)
- ✅ **Script de ingestão** (`scripts/ingestion/`) com suporte a:
  - Carga inicial (full load)
  - Carga incremental (watermark-based)
  - Detecção automática de primeira carga
- ✅ **Tabelas Delta Lake** no Databricks com schema flexível (STRING)
- ✅ **Logging robusto** com Rich para monitoramento

### Características
- 📥 **Append-only**: Preserva histórico completo
- 🔄 **Incremental**: Processa apenas novos/atualizados
- 🛡️ **Idempotente**: MERGE INTO evita duplicações
- 📊 **5 tabelas Bronze** criadas e populadas

---

## SLIDE 4: Camada Silver - Curadoria e Normalização

### O que foi feito
**Objetivo**: Transformar dados brutos em dados confiáveis e prontos para análise

### Processos Implementados

#### 1. Limpeza e Normalização
- ✅ **Parsing de datas** em múltiplos formatos (`yyyy-MM-dd`, `dd-MM-yyyy`, ISO)
- ✅ **Tratamento de NULLs** e valores inconsistentes
- ✅ **Normalização de tipos** (strings → timestamps, números)
- ✅ **Validação de integridade** referencial

#### 2. Deduplicação
- ✅ **Window functions** (`ROW_NUMBER()`) para identificar duplicatas
- ✅ **Chaves de negócio** para identificar registros únicos
- ✅ **Hash SHA256** para detecção de mudanças (idempotência)

#### 3. SCD Type 2 (Slowly Changing Dimension)
- ✅ **Histórico completo** de mudanças em equipamentos
- ✅ **Colunas temporais**: `effective_start`, `effective_end`, `is_current`
- ✅ **Time-aware queries**: permite análise histórica precisa

#### 4. Processamento Incremental
- ✅ **Watermark** (60-90 dias) para janelas incrementais
- ✅ **MERGE INTO** para upsert eficiente
- ✅ **Detecção de mudanças** via hash para evitar updates desnecessários

### Resultado
**6 tabelas Silver** curadas:
- `equipment_clean` + `equipment_scd`
- `iot_readings_clean`
- `production_orders_clean`
- `maintenance_orders_clean`
- `quality_inspections_clean`

---

## SLIDE 5: Camada Gold - Modelo Dimensional Star Schema

### O que é Star Schema?
**Modelo dimensional** onde:
- **Fatos** (centro): Eventos de negócio mensuráveis
- **Dimensões** (raios): Contexto descritivo dos fatos
- **Relacionamentos**: Chaves estrangeiras conectam fatos às dimensões

### Por que Star Schema?
- ⚡ **Performance**: Joins simples e rápidos
- 📊 **Intuitivo**: Fácil de entender para usuários de negócio
- 🔍 **Flexível**: Permite análises multidimensionais
- 📈 **Escalável**: Particionamento por tempo otimiza consultas

---

## SLIDE 6: Dimensões do Modelo (7 Tabelas)

### 1. `dim_tempo` ⏰
**Propósito**: Calendário completo para análise temporal
- `date_key` (INT): Chave surrogate (YYYYMMDD)
- `full_date` (DATE): Data completa
- `day_of_week`, `week_number`, `month`, `quarter`, `year`
- **Uso**: Particionamento e filtros temporais em todas as fact tables

### 2. `dim_equipamento` 🔧
**Propósito**: Snapshot atual de equipamentos (SCD Type 1)
- `equipment_sk` (BIGINT): Chave surrogate
- `equipment_id` (STRING): Chave de negócio (NK)
- `equipment_name`, `equipment_type`, `manufacturer`, `location`, `status`
- **Uso**: Contexto atual dos equipamentos em análises

### 3. `dim_equipamento_scd` 📜
**Propósito**: Histórico completo de equipamentos (SCD Type 2)
- `equipment_sk`, `equipment_id`
- `effective_start`, `effective_end`, `is_current`
- **Uso**: Análises históricas time-aware (ex: "qual era o status do equipamento na data X?")

### 4. `dim_produto` 📦
**Propósito**: Produtos fabricados
- `product_id` (STRING): Chave de negócio
- `product_name`
- **Uso**: Análise de produção por produto

### 5. `dim_tecnico` 👨‍🔧
**Propósito**: Técnicos de manutenção
- `technician_id` (STRING): Chave de negócio
- `technician_name`, `specialization`
- **Uso**: Análise de performance de manutenção por técnico

### 6. `dim_tipo_manutencao` 🔨
**Propósito**: Tipos de manutenção
- `maintenance_type` (STRING): Chave de negócio
- `description`, `typical_duration`
- **Uso**: Análise de downtime por tipo (preventiva, corretiva, preditiva)

### 7. `dim_defeito` ⚠️
**Propósito**: Códigos de defeito
- `defect_code` (STRING): Chave de negócio
- `description`, `severity`
- **Uso**: Análise de qualidade e causas de rejeição

---

## SLIDE 7: Fatos do Modelo (4 Tabelas)

### 1. `fact_producao` 🏭
**Grão**: Uma linha por ordem de produção
**Métricas**:
- `planned_qty`, `actual_qty`, `qty_diff`: Quantidades planejadas vs. reais
- `duration_minutes`: Duração da produção
- `status`: Status da ordem

**Dimensões Relacionadas**:
- `equipment_id` → `dim_equipamento`
- `product_id` → `dim_produto`
- `start_date_key` → `dim_tempo`

**Particionamento**: Por `start_date_key` (otimização temporal)

### 2. `fact_manutencao` 🔧
**Grão**: Uma linha por ordem de manutenção
**Métricas**:
- `downtime_minutes`: Tempo de parada (crítico para OEE)
- `cost_estimate`: Estimativa de custo
- `maintenance_type`: Tipo de manutenção

**Dimensões Relacionadas**:
- `equipment_id` → `dim_equipamento`
- `technician_id` → `dim_tecnico`
- `maintenance_type` → `dim_tipo_manutencao`
- `start_date_key` → `dim_tempo`

**Particionamento**: Por `start_date_key`

### 3. `fact_qualidade` ✅
**Grão**: Uma linha por inspeção de qualidade
**Métricas**:
- `total_quantity`, `failed_quantity`: Quantidades inspecionadas
- `rejection_rate_pct`: Taxa de rejeição
- `passed`: Aprovado/Reprovado
- `defect_codes`: Códigos de defeito encontrados

**Dimensões Relacionadas**:
- `equipment_id` → `dim_equipamento`
- `production_order_id` → `fact_producao` (degenerada)
- `defect_codes` → `dim_defeito` (via parsing)
- `date_key` → `dim_tempo`

**Particionamento**: Por `date_key`

### 4. `fact_iot_agregado` 📡
**Grão**: Uma linha por equipamento + sensor + hora
**Métricas**:
- `avg_reading`, `min_reading`, `max_reading`: Estatísticas agregadas
- `sensor_type`: Tipo de sensor (temperatura, vibração, pressão, etc.)

**Dimensões Relacionadas**:
- `equipment_id` → `dim_equipamento`
- `hour_key` → `dim_tempo` (agregação horária)

**Particionamento**: Por `hour_key`

---

## SLIDE 8: Diagrama Star Schema Completo

```
                    ┌─────────────────┐
                    │   dim_tempo     │
                    │  (date_key)     │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│fact_producao  │   │fact_manutencao│   │fact_qualidade │
│               │   │               │   │               │
│• planned_qty  │   │• downtime    │   │• total_qty   │
│• actual_qty  │   │• cost        │   │• failed_qty  │
│• duration    │   │• type        │   │• rejection%   │
└───────┬───────┘   └───────┬───────┘   └───────┬───────┘
        │                   │                   │
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
                    ┌────────┴────────┐
                    │                │
                    ▼                ▼
        ┌──────────────────┐  ┌──────────────┐
        │dim_equipamento    │  │dim_produto   │
        │(equipment_id)     │  │(product_id)  │
        └──────────────────┘  └──────────────┘
                    │
                    │
        ┌───────────┴───────────┐
        │                        │
        ▼                        ▼
┌──────────────┐        ┌──────────────────┐
│dim_tecnico   │        │dim_tipo_manutencao│
│(technician_id)│       │(maintenance_type) │
└──────────────┘        └──────────────────┘

        ┌──────────────────┐
        │fact_iot_agregado │
        │                  │
        │• avg_reading     │
        │• min_reading     │
        │• max_reading     │
        │• sensor_type     │
        └────────┬─────────┘
                 │
                 ▼
        ┌──────────────────┐
        │dim_equipamento   │
        │(equipment_id)    │
        └──────────────────┘
```

### Relacionamentos Principais

**fact_producao** conecta:
- `equipment_id` → `dim_equipamento`
- `product_id` → `dim_produto`
- `start_date_key` → `dim_tempo`

**fact_manutencao** conecta:
- `equipment_id` → `dim_equipamento`
- `technician_id` → `dim_tecnico`
- `maintenance_type` → `dim_tipo_manutencao`
- `start_date_key` → `dim_tempo`

**fact_qualidade** conecta:
- `equipment_id` → `dim_equipamento`
- `defect_codes` → `dim_defeito` (via parsing)
- `date_key` → `dim_tempo`

**fact_iot_agregado** conecta:
- `equipment_id` → `dim_equipamento`
- `hour_key` → `dim_tempo`

---

## SLIDE 9: Exemplo de Consulta Star Schema

### Query: OEE por Equipamento (últimos 30 dias)

```sql
SELECT 
    d.full_date,
    e.equipment_name,
    e.equipment_type,
    -- Disponibilidade
    COALESCE(SUM(p.duration_minutes), 0) as production_time,
    COALESCE(SUM(m.downtime_minutes), 0) as downtime,
    -- Performance
    COALESCE(SUM(p.planned_qty), 0) as planned_qty,
    COALESCE(SUM(p.actual_qty), 0) as actual_qty,
    -- Qualidade
    COALESCE(SUM(q.total_quantity), 0) as inspected_qty,
    COALESCE(SUM(q.failed_quantity), 0) as failed_qty,
    -- Cálculo OEE
    ROUND(
        (COALESCE(SUM(p.duration_minutes), 0) / 
         NULLIF(COALESCE(SUM(p.duration_minutes), 0) + 
                COALESCE(SUM(m.downtime_minutes), 0), 0)) *
        (COALESCE(SUM(p.actual_qty), 0) / 
         NULLIF(COALESCE(SUM(p.planned_qty), 0), 0)) *
        ((COALESCE(SUM(q.total_quantity), 0) - 
          COALESCE(SUM(q.failed_quantity), 0)) / 
         NULLIF(COALESCE(SUM(q.total_quantity), 0), 0))
    , 4) * 100 as oee_pct
FROM dim_tempo d
LEFT JOIN fact_producao p ON d.date_key = p.start_date_key
LEFT JOIN fact_manutencao m ON d.date_key = m.start_date_key 
    AND p.equipment_id = m.equipment_id
LEFT JOIN fact_qualidade q ON d.date_key = q.date_key 
    AND p.equipment_id = q.equipment_id
JOIN dim_equipamento e ON p.equipment_id = e.equipment_id
WHERE d.full_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY d.full_date, e.equipment_name, e.equipment_type
ORDER BY d.full_date DESC, oee_pct ASC;
```

### Por que funciona bem?
- ✅ **Joins simples**: Apenas chaves estrangeiras
- ✅ **Particionamento**: Filtros por `date_key` são muito rápidos
- ✅ **Agregações eficientes**: Dados pré-agregados nas fact tables
- ✅ **Flexibilidade**: Fácil adicionar novas dimensões

---

## SLIDE 10: Views Analíticas (4 Views)

### 1. `vw_oee_diario` 📊
**Propósito**: Cálculo de OEE diário por equipamento
**Fórmula**: `OEE = Disponibilidade × Performance × Qualidade`
**Dados**: Agrega `fact_producao`, `fact_manutencao`, `fact_qualidade`
**Uso**: Dashboard principal de eficiência

### 2. `vw_downtime_por_causa` ⏱️
**Propósito**: Análise de downtime por tipo de manutenção
**Dados**: Agrega `fact_manutencao` + `dim_tipo_manutencao`
**Uso**: Identificar principais causas de parada

### 3. `vw_equipamentos_criticos` 🔴
**Propósito**: Ranking de equipamentos por criticidade
**Métricas**: Combina downtime, falhas, manutenções
**Uso**: Priorização de ações de manutenção

### 4. `vw_tendencias_sensores` 📈
**Propósito**: Tendências de leituras IoT ao longo do tempo
**Dados**: Agrega `fact_iot_agregado` por equipamento e sensor
**Uso**: Detecção de anomalias e manutenção preditiva

---

## SLIDE 11: Orquestração e Automação

### Databricks Asset Bundle (DAB)
**Arquivo**: `jobs/manufacturing_lakehouse_job.yaml`

### Estrutura do Job
- **21 tarefas** orquestradas com dependências
- **6 tarefas Silver**: Processamento incremental das tabelas curadas
- **15 tarefas Gold**: Dimensões, fatos e views analíticas
- **Serverless Compute**: Execução automática sem gerenciamento de cluster

### Dependências
```
Silver (paralelo após Bronze)
  ├─ equipment_clean
  ├─ equipment_scd
  ├─ iot_readings_clean
  ├─ production_orders_clean
  ├─ maintenance_orders_clean
  └─ quality_inspections_clean

Gold Dimensões (paralelo após Silver)
  ├─ dim_tempo
  ├─ dim_equipamento
  ├─ dim_equipamento_scd
  ├─ dim_produto
  ├─ dim_tecnico
  ├─ dim_tipo_manutencao
  └─ dim_defeito

Gold Fatos (dependem de dimensões)
  ├─ fact_producao
  ├─ fact_manutencao
  ├─ fact_qualidade
  └─ fact_iot_agregado

Gold Views (dependem de fatos)
  ├─ vw_oee_diario
  ├─ vw_downtime_por_causa
  ├─ vw_equipamentos_criticos
  └─ vw_tendencias_sensores
```

### Benefícios
- 🔄 **Execução automatizada** via agendamento
- ⚡ **Paralelização** de tarefas independentes
- 🛡️ **Idempotência** garantida em cada etapa
- 📊 **Monitoramento** via Databricks UI

---

## SLIDE 12: Visualização e Dashboards

### Databricks SQL Dashboards
**Dashboard**: "Manufatura - OEE e Performance"

### Componentes do Dashboard

#### KPIs Principais (4 cards)
1. **OEE Médio (30 dias)**: Métrica agregada de eficiência
2. **Downtime Hoje**: Total de minutos de parada
3. **Equipamentos Críticos**: Contagem de equipamentos com score > 100
4. **Taxa de Qualidade (30 dias)**: Percentual de produtos aprovados

#### Visualizações
- **OEE por Equipamento** (Bar Chart): Comparação de eficiência
- **Evolução OEE** (Line Chart): Tendência temporal
- **Top 10 Equipamentos Críticos** (Table): Ranking detalhado
- **Downtime por Causa** (Pie Chart): Distribuição de paradas
- **Downtime por Equipamento** (Bar Chart): Identificação de gargalos
- **Tendências Sensores** (Line Chart): Monitoramento IoT
- **Tabela OEE Detalhada** (Table): Drill-down completo

### Benefícios
- 📊 **Visibilidade em tempo real** para tomada de decisão
- 🎯 **Foco em ações**: identifica equipamentos problemáticos
- 📈 **Análise de tendências**: previne falhas futuras
- 👥 **Acesso self-service**: dashboards para diferentes perfis

---

## SLIDE 13: Conclusão e Próximos Passos

### O que foi entregue
✅ **Lakehouse completo** seguindo Medallion Architecture  
✅ **5 tabelas Bronze** com dados integrados de múltiplas fontes  
✅ **6 tabelas Silver** curadas e normalizadas  
✅ **11 tabelas Gold** (7 dimensões + 4 fatos) em Star Schema  
✅ **4 views analíticas** prontas para consumo  
✅ **Job orquestrado** com 21 tarefas automatizadas  
✅ **Dashboard interativo** com KPIs e visualizações  

### Impacto no Negócio
- 📊 **Visibilidade**: OEE calculado automaticamente
- 🔧 **Manutenção Preditiva**: Identificação de equipamentos críticos
- 📈 **Otimização**: Redução de downtime através de insights
- 💰 **ROI**: Decisões baseadas em dados, não em intuição

### Tecnologias Utilizadas
- **Databricks**: Plataforma Lakehouse
- **Delta Lake**: Tabelas ACID e versionamento
- **Apache Spark**: Processamento distribuído
- **Unity Catalog**: Governança de dados
- **Star Schema**: Modelo dimensional otimizado
- **Python**: Scripts de geração e ingestão
- **PostgreSQL**: Simulação de sistemas legados

### Próximos Passos
1. **Machine Learning**: Modelos preditivos para falhas
2. **Alertas Automáticos**: Notificações quando OEE cai abaixo do threshold
3. **Integração Real**: Conectar com sistemas ERP/IoT reais
4. **Expansão**: Adicionar mais fontes de dados (manutenção preventiva, estoque)
5. **Otimização**: Fine-tuning de queries e particionamento

### Lições Aprendidas
- 🎯 **Medallion Architecture** facilita governança e manutenção
- ⭐ **Star Schema** acelera análises complexas e é intuitivo
- 🔄 **Idempotência** é essencial para reprocessamento seguro
- 📊 **Modelo dimensional** permite análises multidimensionais flexíveis
- 🛡️ **Delta Lake** garante qualidade e confiabilidade dos dados

---

**Fim da Apresentação**

