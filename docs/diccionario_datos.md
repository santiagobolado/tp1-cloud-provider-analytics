# Diccionario de datos

Generado: 2026-11-18T19:55:27+00:00 desde `src/governance/catalog.py` con `python notebooks/04_diccionario_datos.py`.
No editar a mano: se regenera, y `tests/test_governance.py` falla si una columna
del pipeline no tiene descripción.

| Dataset | Zona | Grano | Partición | Responsable | Retención |
|---|---|---|---|---|---|
| [`bronze.customers_orgs`](#bronzecustomers_orgs) | Bronze | el de la fuente | ingest_date | R2 Ingesta y calidad | 24 meses |
| [`quarantine.customers_orgs`](#quarantinecustomers_orgs) | Quarantine | un registro rechazado | ingest_date, regla | R2 Ingesta y calidad | 6 meses |
| [`bronze.users`](#bronzeusers) | Bronze | el de la fuente | ingest_date | R2 Ingesta y calidad | 24 meses |
| [`quarantine.users`](#quarantineusers) | Quarantine | un registro rechazado | ingest_date, regla | R2 Ingesta y calidad | 6 meses |
| [`bronze.resources`](#bronzeresources) | Bronze | el de la fuente | ingest_date | R2 Ingesta y calidad | 24 meses |
| [`quarantine.resources`](#quarantineresources) | Quarantine | un registro rechazado | ingest_date, regla | R2 Ingesta y calidad | 6 meses |
| [`bronze.support_tickets`](#bronzesupport_tickets) | Bronze | el de la fuente | ingest_date | R2 Ingesta y calidad | 24 meses |
| [`quarantine.support_tickets`](#quarantinesupport_tickets) | Quarantine | un registro rechazado | ingest_date, regla | R2 Ingesta y calidad | 6 meses |
| [`bronze.marketing_touches`](#bronzemarketing_touches) | Bronze | el de la fuente | ingest_date | R2 Ingesta y calidad | 24 meses |
| [`quarantine.marketing_touches`](#quarantinemarketing_touches) | Quarantine | un registro rechazado | ingest_date, regla | R2 Ingesta y calidad | 6 meses |
| [`bronze.nps_surveys`](#bronzenps_surveys) | Bronze | el de la fuente | ingest_date | R2 Ingesta y calidad | 24 meses |
| [`quarantine.nps_surveys`](#quarantinenps_surveys) | Quarantine | un registro rechazado | ingest_date, regla | R2 Ingesta y calidad | 6 meses |
| [`bronze.billing_monthly`](#bronzebilling_monthly) | Bronze | el de la fuente | ingest_date | R2 Ingesta y calidad | 24 meses |
| [`quarantine.billing_monthly`](#quarantinebilling_monthly) | Quarantine | un registro rechazado | ingest_date, regla | R2 Ingesta y calidad | 6 meses |
| [`bronze.usage_events`](#bronzeusage_events) | Bronze | un evento | ingest_date | R2 Ingesta y calidad | 24 meses |
| [`quarantine.usage_events`](#quarantineusage_events) | Quarantine | un evento rechazado | ingest_date, regla | R2 Ingesta y calidad | 6 meses |
| [`silver.customers_orgs`](#silvercustomers_orgs) | Silver | una organización | sin particionar | R3 Procesamiento y analítica | 24 meses |
| [`silver.resources`](#silverresources) | Silver | un recurso | sin particionar | R3 Procesamiento y analítica | 24 meses |
| [`silver.usage_events`](#silverusage_events) | Silver | un evento | event_date, service | R3 Procesamiento y analítica | 24 meses |
| [`quarantine.silver_usage_events`](#quarantinesilver_usage_events) | Quarantine | una regla violada por un evento | regla | R2 Ingesta y calidad | 6 meses |
| [`silver.billing_monthly`](#silverbilling_monthly) | Silver | una factura | month | R3 Procesamiento y analítica | 24 meses |
| [`silver.support_tickets`](#silversupport_tickets) | Silver | un ticket | sin particionar | R3 Procesamiento y analítica | 24 meses |
| [`silver.users`](#silverusers) | Silver | un usuario | sin particionar | R3 Procesamiento y analítica | 24 meses |
| [`silver.marketing_touches`](#silvermarketing_touches) | Silver | una interacción | sin particionar | R3 Procesamiento y analítica | 24 meses |
| [`quarantine.silver_billing_monthly`](#quarantinesilver_billing_monthly) | Quarantine | una regla violada por un registro | regla | R2 Ingesta y calidad | 6 meses |
| [`quarantine.silver_support_tickets`](#quarantinesilver_support_tickets) | Quarantine | una regla violada por un registro | regla | R2 Ingesta y calidad | 6 meses |
| [`quarantine.silver_users`](#quarantinesilver_users) | Quarantine | una regla violada por un registro | regla | R2 Ingesta y calidad | 6 meses |
| [`quarantine.silver_marketing_touches`](#quarantinesilver_marketing_touches) | Quarantine | una regla violada por un registro | regla | R2 Ingesta y calidad | 6 meses |
| [`gold.org_daily_usage_by_service`](#goldorg_daily_usage_by_service) | Gold | org_id, usage_date, service | usage_date | R3 Procesamiento y analítica | 36 meses |
| [`gold.org_daily_usage_by_service_speed`](#goldorg_daily_usage_by_service_speed) | Gold | clave del mart × micro-lote | batch_id | R3 Procesamiento y analítica | vida del job |
| [`gold.cost_anomaly_mart`](#goldcost_anomaly_mart) | Gold | org_id, date, service | date | R3 Procesamiento y analítica | 36 meses |
| [`gold.revenue_by_org_month`](#goldrevenue_by_org_month) | Gold | org_id, month | month | R3 Procesamiento y analítica | 36 meses |
| [`gold.tickets_by_org_date`](#goldtickets_by_org_date) | Gold | org_id, ticket_date, severity | sin particionar | R3 Procesamiento y analítica | 36 meses |
| [`gold.genai_tokens_by_org_date`](#goldgenai_tokens_by_org_date) | Gold | org_id, usage_date | usage_date | R3 Procesamiento y analítica | 36 meses |
| [`cassandra.org_daily_usage_by_service`](#cassandraorg_daily_usage_by_service) | Serving | org_id, usage_date, service | PK ((org_id), usage_date, service) | R4 Serving y reproducibilidad | la del mart |
| [`cassandra.top_services_by_org_14d`](#cassandratop_services_by_org_14d) | Serving | org_id, as_of_date, rank | PK ((org_id, as_of_date), rank) | R4 Serving y reproducibilidad | la del mart |
| [`cassandra.tickets_by_severity_date`](#cassandratickets_by_severity_date) | Serving | severity, ticket_date | PK ((severity), ticket_date) | R4 Serving y reproducibilidad | la del mart |
| [`cassandra.revenue_by_org_month`](#cassandrarevenue_by_org_month) | Serving | org_id, month | PK ((org_id), month) | R4 Serving y reproducibilidad | la del mart |
| [`cassandra.genai_tokens_by_org_date`](#cassandragenai_tokens_by_org_date) | Serving | org_id, usage_date | PK ((org_id), usage_date) | R4 Serving y reproducibilidad | la del mart |

## bronze.customers_orgs

Copia tipada y deduplicada de customers_orgs.csv, una foto por día de ingesta.

- **Ruta:** `bronze/customers_orgs` · **Zona:** Bronze · **Grano:** el de la fuente
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/customers_orgs.csv`
- **Partición:** ingest_date · **Retención:** 24 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `org_name` | string | Nombre de la organización. |
| `industry` | string | Industria de la organización. |
| `hq_region` | string | Región de la casa matriz. |
| `plan_tier` | string | Plan contratado: free, standard, pro o enterprise. |
| `is_enterprise` | boolean | Indica si la organización es cuenta enterprise. |
| `signup_date` | date | Fecha de alta de la organización. |
| `sales_rep` | string | Representante comercial asignado. |
| `lifecycle_stage` | string | Etapa del ciclo de vida: lead, prospect, active, at_risk, churned. |
| `marketing_source` | string | Canal de adquisición de la organización. |
| `nps_score` | double | NPS de la organización (escala -100 a 100). |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | date | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## quarantine.customers_orgs

Filas no parseables, con clave nula o repetida.

- **Ruta:** `quarantine/customers_orgs` · **Zona:** Quarantine · **Grano:** un registro rechazado
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/customers_orgs.csv`
- **Partición:** ingest_date, regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | — | Dataset de origen del registro en quarantine. |
| `regla` | — | Regla de calidad violada (motivo). |
| `detalle` | — | Descripción legible de la violación. |
| `record_json` | — | Registro completo serializado a JSON (lo que se pudo parsear). |
| `raw_record` | — | Línea cruda cuando no se pudo parsear; nulo en otro caso. |
| `source_file` | — | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | — | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | — | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## bronze.users

Copia tipada y deduplicada de users.csv, una foto por día de ingesta.

- **Ruta:** `bronze/users` · **Zona:** Bronze · **Grano:** el de la fuente
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/users.csv`
- **Partición:** ingest_date · **Retención:** 24 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `user_id` | string | Identificador del usuario. |
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `email` | string | Email del usuario (dato personal; ver docs/gobierno.md, acceso). |
| `role` | string | Rol del usuario en la organización. |
| `active` | boolean | Indica si el usuario está activo. |
| `created_at` | date | Fecha de creación del registro en el sistema de origen. |
| `last_login` | date | Último acceso del usuario; nulo si nunca ingresó. |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | date | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## quarantine.users

Filas no parseables, con clave nula o repetida.

- **Ruta:** `quarantine/users` · **Zona:** Quarantine · **Grano:** un registro rechazado
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/users.csv`
- **Partición:** ingest_date, regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | — | Dataset de origen del registro en quarantine. |
| `regla` | — | Regla de calidad violada (motivo). |
| `detalle` | — | Descripción legible de la violación. |
| `record_json` | — | Registro completo serializado a JSON (lo que se pudo parsear). |
| `raw_record` | — | Línea cruda cuando no se pudo parsear; nulo en otro caso. |
| `source_file` | — | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | — | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | — | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## bronze.resources

Copia tipada y deduplicada de resources.csv, una foto por día de ingesta.

- **Ruta:** `bronze/resources` · **Zona:** Bronze · **Grano:** el de la fuente
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/resources.csv`
- **Partición:** ingest_date · **Retención:** 24 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `resource_id` | string | Identificador del recurso cloud. |
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `service` | string | Servicio cloud: compute, storage, database, networking, analytics, genai. |
| `region` | string | Región del recurso o del evento. |
| `created_at` | date | Fecha de creación del registro en el sistema de origen. |
| `state` | string | Estado del recurso: running, stopped, terminated. |
| `tags_json` | string | Etiquetas del recurso como JSON embebido en el CSV (texto). |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | date | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## quarantine.resources

Filas no parseables, con clave nula o repetida.

- **Ruta:** `quarantine/resources` · **Zona:** Quarantine · **Grano:** un registro rechazado
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/resources.csv`
- **Partición:** ingest_date, regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | — | Dataset de origen del registro en quarantine. |
| `regla` | — | Regla de calidad violada (motivo). |
| `detalle` | — | Descripción legible de la violación. |
| `record_json` | — | Registro completo serializado a JSON (lo que se pudo parsear). |
| `raw_record` | — | Línea cruda cuando no se pudo parsear; nulo en otro caso. |
| `source_file` | — | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | — | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | — | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## bronze.support_tickets

Copia tipada y deduplicada de support_tickets.csv, una foto por día de ingesta.

- **Ruta:** `bronze/support_tickets` · **Zona:** Bronze · **Grano:** el de la fuente
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/support_tickets.csv`
- **Partición:** ingest_date · **Retención:** 24 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `ticket_id` | string | Identificador del ticket de soporte. |
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `category` | string | Categoría del ticket. |
| `severity` | string | Severidad del ticket: low, medium, high, critical. |
| `created_at` | date | Fecha de creación del registro en el sistema de origen. |
| `resolved_at` | date | Fecha de resolución; nulo = ticket abierto (nulo semántico). |
| `csat` | double | Satisfacción del cliente; nulo = encuesta no respondida (nulo semántico). |
| `sla_breached` | boolean | Indica si el ticket incumplió el SLA. |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | date | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## quarantine.support_tickets

Filas no parseables, con clave nula o repetida.

- **Ruta:** `quarantine/support_tickets` · **Zona:** Quarantine · **Grano:** un registro rechazado
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/support_tickets.csv`
- **Partición:** ingest_date, regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | — | Dataset de origen del registro en quarantine. |
| `regla` | — | Regla de calidad violada (motivo). |
| `detalle` | — | Descripción legible de la violación. |
| `record_json` | — | Registro completo serializado a JSON (lo que se pudo parsear). |
| `raw_record` | — | Línea cruda cuando no se pudo parsear; nulo en otro caso. |
| `source_file` | — | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | — | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | — | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## bronze.marketing_touches

Copia tipada y deduplicada de marketing_touches.csv, una foto por día de ingesta.

- **Ruta:** `bronze/marketing_touches` · **Zona:** Bronze · **Grano:** el de la fuente
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/marketing_touches.csv`
- **Partición:** ingest_date · **Retención:** 24 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `touch_id` | string | Identificador de la interacción de marketing. |
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `campaign` | string | Campaña de marketing. |
| `channel` | string | Canal de la interacción: email, ads, event, in_app. |
| `timestamp` | date | Fecha o fecha-hora de origen, tal como llega de la fuente. |
| `clicked` | boolean | Indica si hubo clic. |
| `converted` | boolean | Indica si hubo conversión. |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | date | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## quarantine.marketing_touches

Filas no parseables, con clave nula o repetida.

- **Ruta:** `quarantine/marketing_touches` · **Zona:** Quarantine · **Grano:** un registro rechazado
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/marketing_touches.csv`
- **Partición:** ingest_date, regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | — | Dataset de origen del registro en quarantine. |
| `regla` | — | Regla de calidad violada (motivo). |
| `detalle` | — | Descripción legible de la violación. |
| `record_json` | — | Registro completo serializado a JSON (lo que se pudo parsear). |
| `raw_record` | — | Línea cruda cuando no se pudo parsear; nulo en otro caso. |
| `source_file` | — | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | — | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | — | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## bronze.nps_surveys

Copia tipada y deduplicada de nps_surveys.csv, una foto por día de ingesta.

- **Ruta:** `bronze/nps_surveys` · **Zona:** Bronze · **Grano:** el de la fuente
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/nps_surveys.csv`
- **Partición:** ingest_date · **Retención:** 24 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `survey_date` | date | Fecha de la encuesta NPS. |
| `nps_score` | double | NPS de la organización (escala -100 a 100). |
| `comment` | string | Comentario categórico de la encuesta. |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | date | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## quarantine.nps_surveys

Filas no parseables, con clave nula o repetida.

- **Ruta:** `quarantine/nps_surveys` · **Zona:** Quarantine · **Grano:** un registro rechazado
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/nps_surveys.csv`
- **Partición:** ingest_date, regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | — | Dataset de origen del registro en quarantine. |
| `regla` | — | Regla de calidad violada (motivo). |
| `detalle` | — | Descripción legible de la violación. |
| `record_json` | — | Registro completo serializado a JSON (lo que se pudo parsear). |
| `raw_record` | — | Línea cruda cuando no se pudo parsear; nulo en otro caso. |
| `source_file` | — | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | — | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | — | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## bronze.billing_monthly

Copia tipada y deduplicada de billing_monthly.csv, una foto por día de ingesta.

- **Ruta:** `bronze/billing_monthly` · **Zona:** Bronze · **Grano:** el de la fuente
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/billing_monthly.csv`
- **Partición:** ingest_date · **Retención:** 24 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `invoice_id` | string | Identificador de la factura mensual. |
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `month` | date | Mes facturado (primer día del mes). |
| `subtotal` | double | Subtotal de la factura en moneda original. |
| `credits` | double | Créditos aplicados; nulo = sin crédito (D-007). |
| `taxes` | double | Impuestos de la factura en moneda original. |
| `currency` | string | Moneda de la factura: USD, ARS, EUR. |
| `exchange_rate_to_usd` | double | Tipo de cambio a USD informado por la fuente (ruidoso en USD, D-007). |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | date | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## quarantine.billing_monthly

Filas no parseables, con clave nula o repetida.

- **Ruta:** `quarantine/billing_monthly` · **Zona:** Quarantine · **Grano:** un registro rechazado
- **Produce:** `src.ingest.batch_to_bronze` · **Linaje (origen):** `landing/billing_monthly.csv`
- **Partición:** ingest_date, regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | — | Dataset de origen del registro en quarantine. |
| `regla` | — | Regla de calidad violada (motivo). |
| `detalle` | — | Descripción legible de la violación. |
| `record_json` | — | Registro completo serializado a JSON (lo que se pudo parsear). |
| `raw_record` | — | Línea cruda cuando no se pudo parsear; nulo en otro caso. |
| `source_file` | — | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | — | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | — | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## bronze.usage_events

Eventos de uso tipados y deduplicados por event_id (D-013).

- **Ruta:** `bronze/usage_events` · **Zona:** Bronze · **Grano:** un evento
- **Produce:** `src.ingest.stream_to_bronze` · **Linaje (origen):** `landing/usage_events_stream/*.jsonl`
- **Partición:** ingest_date · **Retención:** 24 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `event_id` | string | Identificador del evento de uso; clave de deduplicación. |
| `timestamp` | string | Fecha o fecha-hora de origen, tal como llega de la fuente. |
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `resource_id` | string | Identificador del recurso cloud. |
| `service` | string | Servicio cloud: compute, storage, database, networking, analytics, genai. |
| `region` | string | Región del recurso o del evento. |
| `metric` | string | Métrica medida: requests, cpu_hours, storage_gb_hours. |
| `value` | string | Valor de la métrica como llega (texto o número, D-003). |
| `unit` | string | Unidad de la métrica; imputada desde la métrica si faltaba (D-014). |
| `cost_usd_increment` | double | Costo incremental del evento en USD, tal como llega. |
| `schema_version` | int | Versión del esquema del evento: 1 o 2 (desde 2025-07-18). |
| `carbon_kg` | double | Huella de carbono en kg; solo eventos v2, nulo = no medido. |
| `genai_tokens` | bigint | Tokens GenAI; solo v2 y service = genai. |
| `event_ts` | timestamp | Fecha-hora del evento (event time) parseada a timestamp UTC. |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | date | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## quarantine.usage_events

JSON no parseable, event_id nulo o timestamp inválido.

- **Ruta:** `quarantine/usage_events` · **Zona:** Quarantine · **Grano:** un evento rechazado
- **Produce:** `src.ingest.stream_to_bronze` · **Linaje (origen):** `landing/usage_events_stream/*.jsonl`
- **Partición:** ingest_date, regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | — | Dataset de origen del registro en quarantine. |
| `regla` | — | Regla de calidad violada (motivo). |
| `record_json` | — | Registro completo serializado a JSON (lo que se pudo parsear). |
| `raw_record` | — | Línea cruda cuando no se pudo parsear; nulo en otro caso. |
| `source_file` | — | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | — | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `ingest_date` | — | Fecha de ingesta; clave de partición de Bronze (D-005, D-011). |

## silver.customers_orgs

Dimensión de organizaciones con el NPS resuelto (D-006).

- **Ruta:** `silver/customers_orgs` · **Zona:** Silver · **Grano:** una organización
- **Produce:** `src.transform.silver_job` · **Linaje (origen):** `bronze.customers_orgs`, `bronze.nps_surveys`
- **Partición:** sin particionar · **Retención:** 24 meses · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `org_name` | string | Nombre de la organización. |
| `industry` | string | Industria de la organización. |
| `hq_region` | string | Región de la casa matriz. |
| `plan_tier` | string | Plan contratado: free, standard, pro o enterprise. |
| `is_enterprise` | boolean | Indica si la organización es cuenta enterprise. |
| `signup_date` | date | Fecha de alta de la organización. |
| `sales_rep` | string | Representante comercial asignado. |
| `lifecycle_stage` | string | Etapa del ciclo de vida: lead, prospect, active, at_risk, churned. |
| `marketing_source` | string | Canal de adquisición de la organización. |
| `nps_score_crm` | double | NPS según el CRM (customers_orgs), foto de antigüedad desconocida (D-006). |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `nps_score_last_survey` | double | NPS de la última encuesta con score no nulo (D-006). |
| `nps_survey_date` | date | Fecha de la última encuesta con score. |
| `nps_score` | double | NPS para consumo: última encuesta, si no CRM (D-006). |
| `nps_source` | string | Origen del nps_score: survey, crm o nulo (D-006). |
| `quality_flags` | array<string> | Reglas de calidad marcadas en la fila (no bloqueantes). |

## silver.resources

Recursos con etiquetas parseadas.

- **Ruta:** `silver/resources` · **Zona:** Silver · **Grano:** un recurso
- **Produce:** `src.transform.silver_job` · **Linaje (origen):** `bronze.resources`
- **Partición:** sin particionar · **Retención:** 24 meses · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `resource_id` | string | Identificador del recurso cloud. |
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `service` | string | Servicio cloud: compute, storage, database, networking, analytics, genai. |
| `region` | string | Región del recurso o del evento. |
| `created_at` | date | Fecha de creación del registro en el sistema de origen. |
| `state` | string | Estado del recurso: running, stopped, terminated. |
| `tags` | array<string> | Etiquetas del recurso como array (parseo de tags_json). |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |

## silver.usage_events

Eventos conformados, v1/v2 compatibles, con features y flags de calidad.

- **Ruta:** `silver/usage_events` · **Zona:** Silver · **Grano:** un evento
- **Produce:** `src.transform.silver_job` · **Linaje (origen):** `bronze.usage_events`, `silver.customers_orgs`
- **Partición:** event_date, service · **Retención:** 24 meses · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `event_id` | string | Identificador del evento de uso; clave de deduplicación. |
| `event_ts` | timestamp | Fecha-hora del evento (event time) parseada a timestamp UTC. |
| `event_date` | date | Fecha del evento; clave de partición de Silver. |
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `org_name` | string | Nombre de la organización. |
| `industry` | string | Industria de la organización. |
| `plan_tier` | string | Plan contratado: free, standard, pro o enterprise. |
| `is_enterprise` | boolean | Indica si la organización es cuenta enterprise. |
| `org_hq_region` | string | Región de la casa matriz de la organización (del join con customers_orgs). |
| `resource_id` | string | Identificador del recurso cloud. |
| `service` | string | Servicio cloud: compute, storage, database, networking, analytics, genai. |
| `region` | string | Región del recurso o del evento. |
| `metric` | string | Métrica medida: requests, cpu_hours, storage_gb_hours. |
| `value_raw` | string | Valor de la métrica como llegó a Bronze, sin castear (D-003). |
| `value_num` | double | Valor de la métrica casteado a double; nulo si no hubo valor (D-009). |
| `unit` | string | Unidad de la métrica; imputada desde la métrica si faltaba (D-014). |
| `cost_usd` | double | Costo incremental del evento en USD (puede ser negativo: crédito). |
| `cpu_hours` | double | Feature: horas de CPU. |
| `requests` | double | Feature: cantidad de requests. |
| `storage_gb_hours` | double | Feature: GB-hora de almacenamiento. |
| `genai_tokens` | bigint | Tokens GenAI; solo v2 y service = genai. |
| `carbon_kg` | double | Huella de carbono en kg; solo eventos v2, nulo = no medido. |
| `schema_version` | int | Versión del esquema del evento: 1 o 2 (desde 2025-07-18). |
| `is_credit_adjustment` | boolean | Costo < -0,01 USD: posible nota de crédito (flag, no rechazo). |
| `quality_flags` | array<string> | Reglas de calidad marcadas en la fila (no bloqueantes). |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `silver_ts` | timestamp | Linaje: fecha-hora de la corrida que escribió Silver. |

## quarantine.silver_usage_events

Copias de auditoría de eventos marcados (siguen en Silver).

- **Ruta:** `quarantine/silver_usage_events` · **Zona:** Quarantine · **Grano:** una regla violada por un evento
- **Produce:** `src.transform.silver_job` · **Linaje (origen):** `bronze.usage_events`
- **Partición:** regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | string | Dataset de origen del registro en quarantine. |
| `regla` | string | Regla de calidad violada (motivo). |
| `event_id` | string | Identificador del evento de uso; clave de deduplicación. |
| `record_json` | string | Registro completo serializado a JSON (lo que se pudo parsear). |
| `destino_en_silver` | string | promovido_con_flag (sigue en Silver) o descartado (duplicado). |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |

## silver.billing_monthly

Facturas con montos normalizados a USD (D-007, D-019).

- **Ruta:** `silver/billing_monthly` · **Zona:** Silver · **Grano:** una factura
- **Produce:** `src.transform.silver_job` · **Linaje (origen):** `bronze.billing_monthly`, `silver.customers_orgs`
- **Partición:** month · **Retención:** 24 meses · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `invoice_id` | string | Identificador de la factura mensual. |
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `org_name` | string | Nombre de la organización. |
| `plan_tier` | string | Plan contratado: free, standard, pro o enterprise. |
| `month` | date | Mes facturado (primer día del mes). |
| `currency` | string | Moneda de la factura: USD, ARS, EUR. |
| `subtotal` | double | Subtotal de la factura en moneda original. |
| `credits` | double | Créditos en moneda original; nulo resuelto como 0 (D-007). |
| `taxes` | double | Impuestos de la factura en moneda original. |
| `exchange_rate_to_usd` | double | Tipo de cambio a USD informado por la fuente (ruidoso en USD, D-007). |
| `fx_applied` | double | Tipo de cambio aplicado: 1,0 en USD (D-007); el de la fuente en ARS y EUR (D-019). |
| `credits_usd` | double | Créditos de la factura en USD (credits × fx_applied). |
| `taxes_usd` | double | Impuestos de la factura en USD (taxes × fx_applied). |
| `net_revenue_usd` | double | Revenue neto en USD: (subtotal − credits) × fx_applied (D-019). |
| `total_billed_usd` | double | Total facturado en USD: (subtotal − credits + taxes) × fx_applied (D-019). |
| `is_credit_note` | boolean | Subtotal negativo: nota de crédito o ajuste (flag, no rechazo). |
| `quality_flags` | array<string> | Reglas de calidad marcadas en la fila (no bloqueantes). |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `silver_ts` | timestamp | Linaje: fecha-hora de la corrida que escribió Silver. |

## silver.support_tickets

Tickets conformados con estado, tiempo de resolución y CSAT validado (D-022).

- **Ruta:** `silver/support_tickets` · **Zona:** Silver · **Grano:** un ticket
- **Produce:** `src.transform.silver_job` · **Linaje (origen):** `bronze.support_tickets`, `silver.customers_orgs`
- **Partición:** sin particionar · **Retención:** 24 meses · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `ticket_id` | string | Identificador del ticket de soporte. |
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `org_name` | string | Nombre de la organización. |
| `plan_tier` | string | Plan contratado: free, standard, pro o enterprise. |
| `category` | string | Categoría del ticket. |
| `severity` | string | Severidad del ticket: low, medium, high, critical. |
| `ticket_date` | date | Fecha de apertura del ticket (created_at); fecha del mart de Soporte. |
| `resolved_at` | date | Fecha de resolución; nulo = ticket abierto (nulo semántico). |
| `is_open` | boolean | Ticket abierto: resolved_at nulo (nulo semántico). |
| `resolution_days` | int | Días entre apertura y resolución; nulo si el ticket sigue abierto. |
| `csat` | double | CSAT para consumo (escala 1-5); nulo si no se respondió o si estaba fuera de escala (D-022). |
| `csat_raw` | double | CSAT tal como llega de la fuente, incluso fuera de la escala 1-5 (D-022). |
| `sla_breached` | boolean | Indica si el ticket incumplió el SLA. |
| `quality_flags` | array<string> | Reglas de calidad marcadas en la fila (no bloqueantes). |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `silver_ts` | timestamp | Linaje: fecha-hora de la corrida que escribió Silver. |

## silver.users

Usuarios sin email (minimización) con la invariante last_login >= created_at (D-022).

- **Ruta:** `silver/users` · **Zona:** Silver · **Grano:** un usuario
- **Produce:** `src.transform.silver_job` · **Linaje (origen):** `bronze.users`, `silver.customers_orgs`
- **Partición:** sin particionar · **Retención:** 24 meses · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `user_id` | string | Identificador del usuario. |
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `role` | string | Rol del usuario en la organización. |
| `active` | boolean | Indica si el usuario está activo. |
| `created_at` | date | Fecha de creación del registro en el sistema de origen. |
| `last_login` | date | Último acceso del usuario; nulo si nunca ingresó. |
| `has_logged_in` | boolean | El usuario ingresó al menos una vez (last_login no nulo). |
| `quality_flags` | array<string> | Reglas de calidad marcadas en la fila (no bloqueantes). |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `silver_ts` | timestamp | Linaje: fecha-hora de la corrida que escribió Silver. |

## silver.marketing_touches

Interacciones de marketing con la invariante del embudo converted => clicked (D-022).

- **Ruta:** `silver/marketing_touches` · **Zona:** Silver · **Grano:** una interacción
- **Produce:** `src.transform.silver_job` · **Linaje (origen):** `bronze.marketing_touches`, `silver.customers_orgs`
- **Partición:** sin particionar · **Retención:** 24 meses · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `touch_id` | string | Identificador de la interacción de marketing. |
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `campaign` | string | Campaña de marketing. |
| `channel` | string | Canal de la interacción: email, ads, event, in_app. |
| `touch_date` | date | Fecha de la interacción de marketing (columna timestamp de la fuente). |
| `clicked` | boolean | Indica si hubo clic. |
| `converted` | boolean | Indica si hubo conversión. |
| `quality_flags` | array<string> | Reglas de calidad marcadas en la fila (no bloqueantes). |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |
| `silver_ts` | timestamp | Linaje: fecha-hora de la corrida que escribió Silver. |

## quarantine.silver_billing_monthly

Copias de auditoría de registros de billing_monthly con defectos (siguen en Silver, D-022).

- **Ruta:** `quarantine/silver_billing_monthly` · **Zona:** Quarantine · **Grano:** una regla violada por un registro
- **Produce:** `src.transform.silver_job` · **Linaje (origen):** `bronze.billing_monthly`
- **Partición:** regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | — | Dataset de origen del registro en quarantine. |
| `regla` | — | Regla de calidad violada (motivo). |
| `record_id` | — | Clave natural del registro en quarantine (invoice_id, ticket_id, user_id o touch_id). |
| `record_json` | — | Registro completo serializado a JSON (lo que se pudo parsear). |
| `destino_en_silver` | — | promovido_con_flag (sigue en Silver) o descartado (duplicado). |
| `source_file` | — | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | — | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |

## quarantine.silver_support_tickets

Copias de auditoría de registros de support_tickets con defectos (siguen en Silver, D-022).

- **Ruta:** `quarantine/silver_support_tickets` · **Zona:** Quarantine · **Grano:** una regla violada por un registro
- **Produce:** `src.transform.silver_job` · **Linaje (origen):** `bronze.support_tickets`
- **Partición:** regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | string | Dataset de origen del registro en quarantine. |
| `regla` | string | Regla de calidad violada (motivo). |
| `record_id` | string | Clave natural del registro en quarantine (invoice_id, ticket_id, user_id o touch_id). |
| `record_json` | string | Registro completo serializado a JSON (lo que se pudo parsear). |
| `destino_en_silver` | string | promovido_con_flag (sigue en Silver) o descartado (duplicado). |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |

## quarantine.silver_users

Copias de auditoría de registros de users con defectos (siguen en Silver, D-022).

- **Ruta:** `quarantine/silver_users` · **Zona:** Quarantine · **Grano:** una regla violada por un registro
- **Produce:** `src.transform.silver_job` · **Linaje (origen):** `bronze.users`
- **Partición:** regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | string | Dataset de origen del registro en quarantine. |
| `regla` | string | Regla de calidad violada (motivo). |
| `record_id` | string | Clave natural del registro en quarantine (invoice_id, ticket_id, user_id o touch_id). |
| `record_json` | string | Registro completo serializado a JSON (lo que se pudo parsear). |
| `destino_en_silver` | string | promovido_con_flag (sigue en Silver) o descartado (duplicado). |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |

## quarantine.silver_marketing_touches

Copias de auditoría de registros de marketing_touches con defectos (siguen en Silver, D-022).

- **Ruta:** `quarantine/silver_marketing_touches` · **Zona:** Quarantine · **Grano:** una regla violada por un registro
- **Produce:** `src.transform.silver_job` · **Linaje (origen):** `bronze.marketing_touches`
- **Partición:** regla · **Retención:** 6 meses · **Responsable:** R2 Ingesta y calidad

| Columna | Tipo | Descripción |
|---|---|---|
| `entity` | string | Dataset de origen del registro en quarantine. |
| `regla` | string | Regla de calidad violada (motivo). |
| `record_id` | string | Clave natural del registro en quarantine (invoice_id, ticket_id, user_id o touch_id). |
| `record_json` | string | Registro completo serializado a JSON (lo que se pudo parsear). |
| `destino_en_silver` | string | promovido_con_flag (sigue en Silver) o descartado (duplicado). |
| `source_file` | string | Linaje: ruta del archivo de Landing de origen. |
| `ingest_ts` | timestamp | Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming). |

## gold.org_daily_usage_by_service

Mart FinOps oficial (reconciliado). Responde P1 y P2.

- **Ruta:** `gold/org_daily_usage_by_service` · **Zona:** Gold · **Grano:** org_id, usage_date, service
- **Produce:** `src.transform.gold_job` · **Linaje (origen):** `silver.usage_events`
- **Partición:** usage_date · **Retención:** 36 meses · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `usage_date` | date | Día de uso; clave de partición de Gold. |
| `service` | string | Servicio cloud: compute, storage, database, networking, analytics, genai. |
| `daily_cost_usd` | double | Costo neto del día en USD (incluye créditos). |
| `daily_cost_gross_usd` | double | Costo bruto del día en USD (solo positivos). |
| `credit_adjustment_usd` | double | Suma de créditos del día en USD (negativo o cero). |
| `requests` | double | Feature: cantidad de requests. |
| `cpu_hours` | double | Feature: horas de CPU. |
| `storage_gb_hours` | double | Feature: GB-hora de almacenamiento. |
| `genai_tokens` | bigint | Tokens GenAI; solo v2 y service = genai. |
| `carbon_kg` | double | Huella de carbono en kg; solo eventos v2, nulo = no medido. |
| `event_count` | bigint | Cantidad de eventos agregados. |
| `events_metric_null` | bigint | Eventos con métrica nula cuyo costo sí se agregó (D-009). |
| `org_name` | string | Nombre de la organización. |
| `plan_tier` | string | Plan contratado: free, standard, pro o enterprise. |
| `source` | string | Ruta que produjo el valor: streaming (provisorio) o reconciliacion. |
| `last_reconciled_at` | timestamp | Fecha-hora de la última reconciliación del mart (§6.7). |

## gold.org_daily_usage_by_service_speed

Capa rápida: valores provisorios del streaming, sin tardíos (D-015).

- **Ruta:** `gold/org_daily_usage_by_service_speed` · **Zona:** Gold · **Grano:** clave del mart × micro-lote
- **Produce:** `src.transform.gold_stream` · **Linaje (origen):** `bronze.usage_events`, `silver.customers_orgs`
- **Partición:** batch_id · **Retención:** vida del job · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `usage_date` | date | Día de uso; clave de partición de Gold. |
| `service` | string | Servicio cloud: compute, storage, database, networking, analytics, genai. |
| `daily_cost_usd` | double | Costo neto del día en USD (incluye créditos). |
| `daily_cost_gross_usd` | double | Costo bruto del día en USD (solo positivos). |
| `credit_adjustment_usd` | double | Suma de créditos del día en USD (negativo o cero). |
| `requests` | double | Feature: cantidad de requests. |
| `cpu_hours` | double | Feature: horas de CPU. |
| `storage_gb_hours` | double | Feature: GB-hora de almacenamiento. |
| `genai_tokens` | bigint | Tokens GenAI; solo v2 y service = genai. |
| `carbon_kg` | double | Huella de carbono en kg; solo eventos v2, nulo = no medido. |
| `event_count` | bigint | Cantidad de eventos agregados. |
| `events_metric_null` | bigint | Eventos con métrica nula cuyo costo sí se agregó (D-009). |
| `org_name` | string | Nombre de la organización. |
| `plan_tier` | string | Plan contratado: free, standard, pro o enterprise. |
| `source` | string | Ruta que produjo el valor: streaming (provisorio) o reconciliacion. |
| `processed_at` | timestamp | Fecha-hora en que el micro-lote escribió la fila. |
| `batch_id` | int | Micro-lote de Structured Streaming que escribió la fila. |

## gold.cost_anomaly_mart

Score y flag de anomalía de costo por celda (D-010, D-016). Responde P6.

- **Ruta:** `gold/cost_anomaly_mart` · **Zona:** Gold · **Grano:** org_id, date, service
- **Produce:** `src.analytics.anomalies` · **Linaje (origen):** `gold.org_daily_usage_by_service`
- **Partición:** date · **Retención:** 36 meses · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `date` | date | Día de uso de la celda (org, día, servicio). |
| `service` | string | Servicio cloud: compute, storage, database, networking, analytics, genai. |
| `org_name` | string | Nombre de la organización. |
| `plan_tier` | string | Plan contratado: free, standard, pro o enterprise. |
| `daily_cost_usd` | double | Costo neto del día en USD (incluye créditos). |
| `daily_cost_gross_usd` | double | Costo bruto del día en USD (solo positivos). |
| `baseline_median_usd` | double | Mediana del costo diario bruto de la línea de base. |
| `baseline_mad_usd` | double | MAD del costo diario bruto de la línea de base. |
| `baseline_source` | string | Línea de base usada: serie, servicio o sin_dispersion (D-016). |
| `history_days` | bigint | Días de historia de la serie (org, servicio). |
| `anomaly_score` | double | Score robusto: 0,6745 · (costo - mediana) / MAD. |
| `is_anomaly` | boolean | Anomalía si |score| > 3,5 (D-010). |
| `direction` | string | pico o caida; nulo si no es anomalía. |
| `has_credit_adjustment` | boolean | El día tuvo créditos (se reportan aparte, no compiten). |
| `credit_adjustment_usd` | double | Suma de créditos del día en USD (negativo o cero). |
| `event_count` | bigint | Cantidad de eventos agregados. |
| `method` | string | Método y umbral aplicados. |

## gold.revenue_by_org_month

Mart FinOps de revenue mensual en USD con créditos, impuestos y FX (D-019). Responde P4.

- **Ruta:** `gold/revenue_by_org_month` · **Zona:** Gold · **Grano:** org_id, month
- **Produce:** `src.transform.gold_domains_job` · **Linaje (origen):** `silver.billing_monthly`
- **Partición:** month · **Retención:** 36 meses · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `month` | date | Mes facturado (primer día del mes). |
| `org_name` | string | Nombre de la organización. |
| `plan_tier` | string | Plan contratado: free, standard, pro o enterprise. |
| `currency` | string | Moneda(s) de las facturas del mes, separadas por coma. |
| `fx_applied` | double | Tipo de cambio aplicado en el mes; nulo si el mes mezcla tasas (D-019). |
| `subtotal_usd` | double | Subtotal del mes en USD (subtotal × fx_applied). |
| `credits_usd` | double | Créditos de la factura en USD (credits × fx_applied). |
| `taxes_usd` | double | Impuestos de la factura en USD (taxes × fx_applied). |
| `net_revenue_usd` | double | Revenue neto en USD: (subtotal − credits) × fx_applied (D-019). |
| `total_billed_usd` | double | Total facturado en USD: (subtotal − credits + taxes) × fx_applied (D-019). |
| `invoice_count` | bigint | Cantidad de facturas agregadas. |
| `has_credit_note` | boolean | El mes incluye al menos una nota de crédito (subtotal < 0). |

## gold.tickets_by_org_date

Mart de Soporte: volumen, SLA y CSAT por día de apertura y severidad. Responde P3.

- **Ruta:** `gold/tickets_by_org_date` · **Zona:** Gold · **Grano:** org_id, ticket_date, severity
- **Produce:** `src.transform.gold_domains_job` · **Linaje (origen):** `silver.support_tickets`
- **Partición:** sin particionar · **Retención:** 36 meses · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `ticket_date` | date | Fecha de apertura del ticket (created_at); fecha del mart de Soporte. |
| `severity` | string | Severidad del ticket: low, medium, high, critical. |
| `org_name` | string | Nombre de la organización. |
| `plan_tier` | string | Plan contratado: free, standard, pro o enterprise. |
| `ticket_count` | bigint | Tickets abiertos ese día (por fecha de apertura). |
| `open_count` | bigint | De esos tickets, cuántos siguen abiertos (resolved_at nulo). |
| `sla_breached_count` | bigint | De esos tickets, cuántos incumplieron el SLA. |
| `csat_responses` | bigint | Tickets con CSAT válido (1-5, D-022). |
| `csat_sum` | double | Suma del CSAT válido; permite re-agregar el promedio. |
| `sla_breach_rate` | double | sla_breached_count / ticket_count. |
| `csat_avg` | double | CSAT promedio: csat_sum / csat_responses; nulo si no hubo respuestas. |

## gold.genai_tokens_by_org_date

Mart de Producto: tokens GenAI y costo estimado por día, desde 2025-07-18 (D-020). Responde P5.

- **Ruta:** `gold/genai_tokens_by_org_date` · **Zona:** Gold · **Grano:** org_id, usage_date
- **Produce:** `src.transform.gold_domains_job` · **Linaje (origen):** `silver.usage_events`
- **Partición:** usage_date · **Retención:** 36 meses · **Responsable:** R3 Procesamiento y analítica

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | string | Identificador de la organización cliente (FK a customers_orgs). |
| `usage_date` | date | Día de uso; clave de partición de Gold. |
| `org_name` | string | Nombre de la organización. |
| `plan_tier` | string | Plan contratado: free, standard, pro o enterprise. |
| `genai_tokens` | bigint | Tokens GenAI; solo v2 y service = genai. |
| `genai_cost_usd` | double | Costo estimado en USD de los eventos GenAI que reportan tokens. |
| `event_count` | bigint | Eventos GenAI v2 agregados (los que reportan tokens). |
| `cost_per_1k_tokens_usd` | double | genai_cost_usd × 1.000 / genai_tokens. |

## cassandra.org_daily_usage_by_service

Tabla de serving de P1 (D-017).

- **Ruta:** `cloud_analytics.org_daily_usage_by_service` · **Zona:** Serving · **Grano:** org_id, usage_date, service
- **Produce:** `src.serving.load_cassandra` · **Linaje (origen):** `gold.org_daily_usage_by_service`, `gold.org_daily_usage_by_service_speed`
- **Partición:** PK ((org_id), usage_date, service) · **Retención:** la del mart · **Responsable:** R4 Serving y reproducibilidad

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | text | Identificador de la organización cliente (FK a customers_orgs). |
| `usage_date` | date | Día de uso; clave de partición de Gold. |
| `service` | text | Servicio cloud: compute, storage, database, networking, analytics, genai. |
| `org_name` | text | Nombre de la organización. |
| `plan_tier` | text | Plan contratado: free, standard, pro o enterprise. |
| `daily_cost_usd` | double | Costo neto del día en USD (incluye créditos). |
| `daily_cost_gross_usd` | double | Costo bruto del día en USD (solo positivos). |
| `credit_adjustment_usd` | double | Suma de créditos del día en USD (negativo o cero). |
| `requests` | double | Feature: cantidad de requests. |
| `cpu_hours` | double | Feature: horas de CPU. |
| `storage_gb_hours` | double | Feature: GB-hora de almacenamiento. |
| `genai_tokens` | bigint | Tokens GenAI; solo v2 y service = genai. |
| `carbon_kg` | double | Huella de carbono en kg; solo eventos v2, nulo = no medido. |
| `event_count` | bigint | Cantidad de eventos agregados. |
| `source` | text | Ruta que produjo el valor: streaming (provisorio) o reconciliacion. |
| `last_reconciled_at` | timestamp | Fecha-hora de la última reconciliación del mart (§6.7). |
| `loaded_at` | timestamp | Fecha-hora de carga en Cassandra. |

## cassandra.top_services_by_org_14d

Tabla de serving de P2 (D-017).

- **Ruta:** `cloud_analytics.top_services_by_org_14d` · **Zona:** Serving · **Grano:** org_id, as_of_date, rank
- **Produce:** `src.serving.load_cassandra` · **Linaje (origen):** `gold.org_daily_usage_by_service`
- **Partición:** PK ((org_id, as_of_date), rank) · **Retención:** la del mart · **Responsable:** R4 Serving y reproducibilidad

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | text | Identificador de la organización cliente (FK a customers_orgs). |
| `as_of_date` | date | Fecha de corte de la ventana de 14 días. |
| `rank` | int | Posición del servicio por costo en la ventana (1 = más caro). |
| `service` | text | Servicio cloud: compute, storage, database, networking, analytics, genai. |
| `cost_14d_usd` | double | Costo neto acumulado en los 14 días, en USD. |
| `requests_14d` | double | Requests acumulados en los 14 días. |
| `days_with_usage` | int | Días con uso del servicio dentro de la ventana. |
| `window_start` | date | Primer día de la ventana de 14 días. |
| `loaded_at` | timestamp | Fecha-hora de carga en Cassandra. |

## cassandra.tickets_by_severity_date

Tabla de serving de P3, con la partición de total 'all' y días sin tickets en 0 (D-018).

- **Ruta:** `cloud_analytics.tickets_by_severity_date` · **Zona:** Serving · **Grano:** severity, ticket_date
- **Produce:** `src.serving.load_cassandra` · **Linaje (origen):** `gold.tickets_by_org_date`
- **Partición:** PK ((severity), ticket_date) · **Retención:** la del mart · **Responsable:** R4 Serving y reproducibilidad

| Columna | Tipo | Descripción |
|---|---|---|
| `severity` | text | Severidad del ticket, o 'all' para el total de todas las severidades. |
| `ticket_date` | date | Fecha de apertura del ticket (created_at); fecha del mart de Soporte. |
| `ticket_count` | int | Tickets abiertos ese día con esa severidad (0 si no hubo). |
| `open_count` | int | De esos tickets, cuántos siguen abiertos (resolved_at nulo). |
| `sla_breached_count` | int | De esos tickets, cuántos incumplieron el SLA. |
| `sla_breach_rate` | double | sla_breached_count / ticket_count. |
| `csat_responses` | int | Tickets con CSAT válido (1-5, D-022). |
| `csat_avg` | double | CSAT promedio: csat_sum / csat_responses; nulo si no hubo respuestas. |
| `org_count` | int | Organizaciones con tickets ese día. |
| `loaded_at` | timestamp | Fecha-hora de carga en Cassandra. |

## cassandra.revenue_by_org_month

Tabla de serving de P4, con la partición de total org_id = 'all' (D-024).

- **Ruta:** `cloud_analytics.revenue_by_org_month` · **Zona:** Serving · **Grano:** org_id, month
- **Produce:** `src.serving.load_cassandra` · **Linaje (origen):** `gold.revenue_by_org_month`
- **Partición:** PK ((org_id), month) · **Retención:** la del mart · **Responsable:** R4 Serving y reproducibilidad

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | text | Organización, o 'all' para el total de la plataforma. |
| `month` | date | Mes facturado (primer día del mes). |
| `org_name` | text | Nombre de la organización. |
| `plan_tier` | text | Plan contratado: free, standard, pro o enterprise. |
| `currency` | text | Moneda(s) facturadas en el mes, separadas por coma. |
| `fx_applied` | double | Tipo de cambio aplicado; nulo si el mes mezcla monedas (siempre en 'all'). |
| `subtotal_usd` | double | Subtotal del mes en USD (subtotal × fx_applied). |
| `credits_usd` | double | Créditos de la factura en USD (credits × fx_applied). |
| `taxes_usd` | double | Impuestos de la factura en USD (taxes × fx_applied). |
| `net_revenue_usd` | double | Revenue neto en USD: (subtotal − credits) × fx_applied (D-019). |
| `total_billed_usd` | double | Total facturado en USD: (subtotal − credits + taxes) × fx_applied (D-019). |
| `invoice_count` | int | Cantidad de facturas agregadas. |
| `has_credit_note` | boolean | El mes incluye al menos una nota de crédito (subtotal < 0). |
| `org_count` | int | Organizaciones agregadas en la fila (1, o todas en 'all'). |
| `loaded_at` | timestamp | Fecha-hora de carga en Cassandra. |

## cassandra.genai_tokens_by_org_date

Tabla de serving de P5, con la partición de total org_id = 'all' (D-024).

- **Ruta:** `cloud_analytics.genai_tokens_by_org_date` · **Zona:** Serving · **Grano:** org_id, usage_date
- **Produce:** `src.serving.load_cassandra` · **Linaje (origen):** `gold.genai_tokens_by_org_date`
- **Partición:** PK ((org_id), usage_date) · **Retención:** la del mart · **Responsable:** R4 Serving y reproducibilidad

| Columna | Tipo | Descripción |
|---|---|---|
| `org_id` | text | Organización, o 'all' para el total de la plataforma. |
| `usage_date` | date | Día de uso; clave de partición de Gold. |
| `org_name` | text | Nombre de la organización. |
| `plan_tier` | text | Plan contratado: free, standard, pro o enterprise. |
| `genai_tokens` | bigint | Tokens GenAI; solo v2 y service = genai. |
| `genai_cost_usd` | double | Costo estimado en USD de los eventos GenAI que reportan tokens. |
| `cost_per_1k_tokens_usd` | double | genai_cost_usd × 1.000 / genai_tokens. |
| `event_count` | int | Eventos GenAI v2 agregados (los que reportan tokens). |
| `org_count` | int | Organizaciones con uso de GenAI agregadas en la fila. |
| `loaded_at` | timestamp | Fecha-hora de carga en Cassandra. |

