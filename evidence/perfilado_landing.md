# Evidencia de exploracion de datos - Landing

Generado: 2026-09-21T02:41:54+00:00
Motor: Apache Spark 4.2.0 (PySpark, master=local[*])
Origen: `data/landing` (lectura read-only, Landing inmutable)

> Todas las cifras de este documento son reproducibles ejecutando `notebooks/01_perfilado_fuentes.py`.

## customers_orgs.csv - Dimension de organizaciones

Filas: **80**  ·  Columnas: **11**

| columna | tipo | nulos | vacios | pct_faltante | distintos |
|---|---|---|---|---|---|
| org_id | string | 0 | 0 | 0.0 | 80 |
| org_name | string | 0 | 0 | 0.0 | 80 |
| industry | string | 0 | 0 | 0.0 | 10 |
| hq_region | string | 0 | 0 | 0.0 | 7 |
| plan_tier | string | 0 | 0 | 0.0 | 4 |
| is_enterprise | boolean | 0 | 0 | 0.0 | 2 |
| signup_date | date | 0 | 0 | 0.0 | 40 |
| sales_rep | string | 0 | 0 | 0.0 | 5 |
| lifecycle_stage | string | 0 | 0 | 0.0 | 5 |
| marketing_source | string | 0 | 0 | 0.0 | 5 |
| nps_score | double | 11 | 0 | 13.75 | 45 |

**Dominio de `plan_tier`**: `standard` (41), `pro` (21), `enterprise` (10), `free` (8)

**Dominio de `hq_region`**: `eu-central` (16), `ap-south` (15), `us-east` (14), `ap-northeast` (11), `sa-east` (10), `us-west` (7), `eu-west` (7)

**Dominio de `lifecycle_stage`**: `active` (54), `at_risk` (11), `churned` (6), `prospect` (6), `lead` (3)

**Rango de `signup_date`**: 2025-05-04 -> 2025-07-02

**Anomalias**: `nps_score` en [-38.0, 101.0] (escala NPS valida: -100 a 100) -> fuera de rango: **1**  ·  nulo: **11**  ·  `org_id` duplicados: **0**

## users.csv - Usuarios por organizacion

Filas: **800**  ·  Columnas: **7**

| columna | tipo | nulos | vacios | pct_faltante | distintos |
|---|---|---|---|---|---|
| user_id | string | 0 | 0 | 0.0 | 800 |
| org_id | string | 0 | 0 | 0.0 | 80 |
| email | string | 0 | 0 | 0.0 | 800 |
| role | string | 0 | 0 | 0.0 | 6 |
| active | boolean | 0 | 0 | 0.0 | 2 |
| created_at | date | 0 | 0 | 0.0 | 100 |
| last_login | date | 139 | 0 | 17.38 | 110 |

**Dominio de `role`**: `data_engineer` (201), `devops` (162), `developer` (130), `analyst` (113), `ml_engineer` (105), `admin` (89)

**Rango de `created_at`**: 2025-05-04 -> 2025-08-11

**Rango de `last_login`**: 2025-05-14 -> 2025-08-31

**Anomalias**: usuarios sin `last_login`: **139**  ·  `last_login` anterior a `created_at`: **232** de 661 con login registrado (35.1%) -> violacion de invariante temporal, no un nulo

## resources.csv - Recursos cloud

Filas: **400**  ·  Columnas: **7**

| columna | tipo | nulos | vacios | pct_faltante | distintos |
|---|---|---|---|---|---|
| resource_id | string | 0 | 0 | 0.0 | 400 |
| org_id | string | 0 | 0 | 0.0 | 80 |
| service | string | 0 | 0 | 0.0 | 6 |
| region | string | 0 | 0 | 0.0 | 7 |
| created_at | date | 0 | 0 | 0.0 | 106 |
| state | string | 0 | 0 | 0.0 | 3 |
| tags_json | string | 83 | 0 | 20.75 | 16 |

**Dominio de `service`**: `compute` (116), `storage` (71), `database` (68), `networking` (64), `analytics` (42), `genai` (39)

**Dominio de `region`**: `ap-northeast` (72), `us-east` (69), `us-west` (61), `sa-east` (57), `ap-south` (55), `eu-west` (46), `eu-central` (40)

**Dominio de `state`**: `running` (242), `stopped` (119), `terminated` (39)

**Rango de `created_at`**: 2025-05-04 -> 2025-08-21

**Anomalias**: `tags_json` nulo (campo semiestructurado a parsear): **83**

## support_tickets.csv - Tickets de soporte

Filas: **1000**  ·  Columnas: **8**

| columna | tipo | nulos | vacios | pct_faltante | distintos |
|---|---|---|---|---|---|
| ticket_id | string | 0 | 0 | 0.0 | 1000 |
| org_id | string | 0 | 0 | 0.0 | 80 |
| category | string | 0 | 0 | 0.0 | 6 |
| severity | string | 0 | 0 | 0.0 | 4 |
| created_at | date | 0 | 0 | 0.0 | 115 |
| resolved_at | date | 240 | 0 | 24.0 | 125 |
| csat | double | 254 | 0 | 25.4 | 8 |
| sla_breached | boolean | 0 | 0 | 0.0 | 2 |

**Dominio de `category`**: `integration` (181), `billing` (180), `availability` (165), `usability` (161), `performance` (158), `security` (155)

**Dominio de `severity`**: `low` (412), `medium` (328), `high` (204), `critical` (56)

**Rango de `created_at`**: 2025-05-09 -> 2025-08-31

**Anomalias**: abiertos (`resolved_at` nulo): **240**  ·  `csat` nulo: **254**  ·  SLA incumplido: **95**  ·  `resolved_at` < `created_at`: **0**

## marketing_touches.csv - Interacciones de marketing

Filas: **1500**  ·  Columnas: **7**

| columna | tipo | nulos | vacios | pct_faltante | distintos |
|---|---|---|---|---|---|
| touch_id | string | 0 | 0 | 0.0 | 1500 |
| org_id | string | 0 | 0 | 0.0 | 80 |
| campaign | string | 0 | 0 | 0.0 | 6 |
| channel | string | 0 | 0 | 0.0 | 4 |
| timestamp | date | 0 | 0 | 0.0 | 120 |
| clicked | boolean | 0 | 0 | 0.0 | 2 |
| converted | boolean | 0 | 0 | 0.0 | 2 |

**Dominio de `channel`**: `event` (401), `email` (385), `ads` (358), `in_app` (356)

**Dominio de `campaign`**: `upgrade_enterprise` (271), `webinar_finops` (263), `genai_launch` (252), `welcome` (247), `upgrade_pro` (245), `security_week` (222)

**Rango de `timestamp`**: 2025-05-04 -> 2025-08-31

**Anomalias**: `converted=True` sin `clicked=True` (inconsistencia logica del embudo): **96**

## nps_surveys.csv - Encuestas NPS

Filas: **92**  ·  Columnas: **4**

| columna | tipo | nulos | vacios | pct_faltante | distintos |
|---|---|---|---|---|---|
| org_id | string | 0 | 0 | 0.0 | 60 |
| survey_date | date | 0 | 0 | 0.0 | 57 |
| nps_score | double | 19 | 0 | 20.65 | 41 |
| comment | string | 10 | 0 | 10.87 | 6 |

**Rango de `survey_date`**: 2025-05-24 -> 2025-08-31

**Dominio de `comment`**: `Missing features` (20), `Love genAI features` (14), `Stable but slow` (13), `Complex billing` (13), `Too expensive` (12), `Great support` (10), `None` (10)

**Anomalias**: organizaciones encuestadas: **60**  ·  con mas de una encuesta (serie temporal -> SCD/ultimo valor): **32**  ·  score fuera de rango: **0**

## billing_monthly.csv - Facturacion mensual

Filas: **240**  ·  Columnas: **8**

| columna | tipo | nulos | vacios | pct_faltante | distintos |
|---|---|---|---|---|---|
| invoice_id | string | 0 | 0 | 0.0 | 240 |
| org_id | string | 0 | 0 | 0.0 | 80 |
| month | date | 0 | 0 | 0.0 | 3 |
| subtotal | double | 0 | 0 | 0.0 | 240 |
| credits | double | 137 | 0 | 57.08 | 100 |
| taxes | double | 0 | 0 | 0.0 | 240 |
| currency | string | 0 | 0 | 0.0 | 3 |
| exchange_rate_to_usd | double | 0 | 0 | 0.0 | 208 |

**Dominio de `currency`**: `USD` (160), `ARS` (51), `EUR` (29)

**Dominio de `month`**: `2025-07-01` (80), `2025-08-01` (80), `2025-06-01` (80)

**Anomalias**: `credits` nulo: **137**  ·  `subtotal` negativo: **13**  ·  rango `exchange_rate_to_usd`: **0.00133 - 1.19808**  ·  filas en USD con FX != 1.0: **160**

## usage_events_stream/*.jsonl - Eventos de uso (fuente de streaming)

Archivos leidos: **1**  ·  Eventos: **360**

> Esquema declarado explicitamente. `value` se lee como String a proposito: la fuente lo emite indistintamente como numero o como texto.

| columna | tipo | nulos | vacios | pct_faltante | distintos |
|---|---|---|---|---|---|
| event_id | string | 0 | 0 | 0.0 | 360 |
| timestamp | string | 0 | 0 | 0.0 | 359 |
| org_id | string | 0 | 0 | 0.0 | 78 |
| resource_id | string | 0 | 0 | 0.0 | 228 |
| service | string | 0 | 0 | 0.0 | 6 |
| region | string | 0 | 0 | 0.0 | 7 |
| metric | string | 0 | 0 | 0.0 | 3 |
| value | string | 9 | 0 | 2.5 | 227 |
| unit | string | 18 | 0 | 5.0 | 3 |
| cost_usd_increment | double | 0 | 0 | 0.0 | 336 |
| schema_version | int | 0 | 0 | 0.0 | 2 |
| carbon_kg | double | 85 | 0 | 23.61 | 176 |
| genai_tokens | bigint | 336 | 0 | 93.33 | 24 |

**Dominio de `service`**: `compute` (104), `database` (68), `storage` (67), `networking` (59), `genai` (32), `analytics` (30)

**Dominio de `metric`**: `requests` (151), `storage_gb_hours` (119), `cpu_hours` (90)

**Dominio de `unit`**: `count` (146), `gb_hours` (113), `hours` (83), `None` (18)

**Dominio de `region`**: `us-east` (76), `ap-northeast` (59), `sa-east` (55), `us-west` (49), `eu-west` (43), `ap-south` (43), `eu-central` (35)

**Rango de `ts`**: 2025-07-03 05:32:00 -> 2025-08-31 15:15:00

### Evolucion de esquema

| schema_version | eventos | desde | hasta | con_carbon_kg | con_genai_tokens |
|---|---|---|---|---|---|
| 1 | 85 | 2025-07-03 05:32:00 | 2025-07-17 20:32:00 | 0 | 0 |
| 2 | 275 | 2025-07-18 04:15:00 | 2025-08-31 15:15:00 | 275 | 24 |

**Regla derivada**: `genai_tokens` esta presente en **24/24** de los eventos `service=genai` con `schema_version=2`, y en **0** eventos de otros servicios. La condicion de presencia es `schema_version=2 AND service='genai'`, no solo la version.

### Problemas de calidad detectados

- `value_nulo`: **9**
- `value_no_casteable_a_double`: **0**
- `value_no_numerico_por_regex`: **0**
- `unit_nulo_con_value_presente`: **17**
- `cost_usd_increment_menor_a_-0.01`: **2**
- `cost_usd_increment_minimo`: **-19.8245**
- `event_id_duplicados`: **0**

**Distribucion de `cost_usd_increment`**: media 2.5912, desvio 4.0111, p50 0.7426, p95 10.7290, p99 15.3517. Eventos con |z| > 3: **6**.

### Fragmentacion de archivos y llegada tardia

| archivo | eventos | ts_min | ts_max | span_dias |
|---|---|---|---|---|
| events_part_0104.jsonl | 360 | 2025-07-03 05:32:00 | 2025-08-31 15:15:00 | 59 |

> Cada archivo cubre practicamente la ventana completa del dataset en lugar de un corte cronologico. La fragmentacion es aleatoria, no temporal: cada micro-lote traera eventos antiguos y recientes mezclados. Esto obliga a definir watermark y politica de late data, y hace que la deduplicacion por `event_id` deba ser inter-lote y no solo intra-lote.

## Integridad referencial

| fuente | orgs_distintas | org_id_sin_match_en_customers_orgs |
|---|---|---|
| users | 80 | 0 |
| resources | 80 | 0 |
| support_tickets | 80 | 0 |
| marketing_touches | 80 | 0 |
| nps_surveys | 60 | 0 |
| billing_monthly | 80 | 0 |
| usage_events | 78 | 0 |

`resource_id` de eventos sin match en `resources.csv`: **0** (de 228 distintos).
