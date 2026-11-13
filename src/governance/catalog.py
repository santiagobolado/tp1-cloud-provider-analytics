"""Catálogo de datos: metadatos de gobierno como código (consigna §6.2, §4.4).

Una sola fuente para tres cosas:
  - el diccionario de datos (`notebooks/04_diccionario_datos.py` lo genera);
  - los responsables, el linaje entre zonas y la retención por dataset;
  - un control automático: `tests/test_governance.py` falla si una tabla del
    pipeline tiene una columna sin descripción. El catálogo no puede quedar
    desactualizado sin que se note.

Responsables (roles del TP1, §12.1):
  R1 Arquitectura y gobierno · R2 Ingesta y calidad ·
  R3 Procesamiento y analítica · R4 Serving y reproducibilidad
"""

# ---------------------------------------------------------------------------
# Columnas. Mismo nombre = mismo significado en todo el lake; las excepciones
# van en `overrides` del dataset.
# ---------------------------------------------------------------------------
COLUMN_DOCS = {
    # claves e identificadores
    "org_id": "Identificador de la organización cliente (FK a customers_orgs).",
    "user_id": "Identificador del usuario.",
    "resource_id": "Identificador del recurso cloud.",
    "ticket_id": "Identificador del ticket de soporte.",
    "touch_id": "Identificador de la interacción de marketing.",
    "invoice_id": "Identificador de la factura mensual.",
    "event_id": "Identificador del evento de uso; clave de deduplicación.",
    # organización
    "org_name": "Nombre de la organización.",
    "industry": "Industria de la organización.",
    "hq_region": "Región de la casa matriz.",
    "org_hq_region": "Región de la casa matriz de la organización (del join con customers_orgs).",
    "plan_tier": "Plan contratado: free, standard, pro o enterprise.",
    "is_enterprise": "Indica si la organización es cuenta enterprise.",
    "signup_date": "Fecha de alta de la organización.",
    "sales_rep": "Representante comercial asignado.",
    "lifecycle_stage": "Etapa del ciclo de vida: lead, prospect, active, at_risk, churned.",
    "marketing_source": "Canal de adquisición de la organización.",
    "nps_score": "NPS de la organización (escala -100 a 100).",
    "nps_score_crm": "NPS según el CRM (customers_orgs), foto de antigüedad desconocida (D-006).",
    "nps_score_last_survey": "NPS de la última encuesta con score no nulo (D-006).",
    "nps_survey_date": "Fecha de la última encuesta con score.",
    "nps_source": "Origen del nps_score: survey, crm o nulo (D-006).",
    # usuarios, recursos, tickets, marketing, encuestas, facturación
    "email": "Email del usuario (dato personal; ver docs/gobierno.md, acceso).",
    "role": "Rol del usuario en la organización.",
    "active": "Indica si el usuario está activo.",
    "created_at": "Fecha de creación del registro en el sistema de origen.",
    "last_login": "Último acceso del usuario; nulo si nunca ingresó.",
    "service": "Servicio cloud: compute, storage, database, networking, analytics, genai.",
    "region": "Región del recurso o del evento.",
    "state": "Estado del recurso: running, stopped, terminated.",
    "tags_json": "Etiquetas del recurso como JSON embebido en el CSV (texto).",
    "tags": "Etiquetas del recurso como array (parseo de tags_json).",
    "category": "Categoría del ticket.",
    "severity": "Severidad del ticket: low, medium, high, critical.",
    "resolved_at": "Fecha de resolución; nulo = ticket abierto (nulo semántico).",
    "csat": "Satisfacción del cliente; nulo = encuesta no respondida (nulo semántico).",
    "sla_breached": "Indica si el ticket incumplió el SLA.",
    "campaign": "Campaña de marketing.",
    "channel": "Canal de la interacción: email, ads, event, in_app.",
    "timestamp": "Fecha o fecha-hora de origen, tal como llega de la fuente.",
    "clicked": "Indica si hubo clic.",
    "converted": "Indica si hubo conversión.",
    "survey_date": "Fecha de la encuesta NPS.",
    "comment": "Comentario categórico de la encuesta.",
    "month": "Mes facturado (primer día del mes).",
    "subtotal": "Subtotal de la factura en moneda original.",
    "credits": "Créditos aplicados; nulo = sin crédito (D-007).",
    "taxes": "Impuestos de la factura en moneda original.",
    "currency": "Moneda de la factura: USD, ARS, EUR.",
    "exchange_rate_to_usd": "Tipo de cambio a USD informado por la fuente (ruidoso en USD, D-007).",
    # eventos
    "metric": "Métrica medida: requests, cpu_hours, storage_gb_hours.",
    "value": "Valor de la métrica como llega (texto o número, D-003).",
    "value_raw": "Valor de la métrica como llegó a Bronze, sin castear (D-003).",
    "value_num": "Valor de la métrica casteado a double; nulo si no hubo valor (D-009).",
    "unit": "Unidad de la métrica; imputada desde la métrica si faltaba (D-014).",
    "cost_usd_increment": "Costo incremental del evento en USD, tal como llega.",
    "cost_usd": "Costo incremental del evento en USD (puede ser negativo: crédito).",
    "schema_version": "Versión del esquema del evento: 1 o 2 (desde 2025-07-18).",
    "carbon_kg": "Huella de carbono en kg; solo eventos v2, nulo = no medido.",
    "genai_tokens": "Tokens GenAI; solo v2 y service = genai.",
    "event_ts": "Fecha-hora del evento (event time) parseada a timestamp UTC.",
    "event_date": "Fecha del evento; clave de partición de Silver.",
    "requests": "Feature: cantidad de requests.",
    "cpu_hours": "Feature: horas de CPU.",
    "storage_gb_hours": "Feature: GB-hora de almacenamiento.",
    "is_credit_adjustment": "Costo < -0,01 USD: posible nota de crédito (flag, no rechazo).",
    # columnas técnicas y de linaje
    "source_file": "Linaje: ruta del archivo de Landing de origen.",
    "ingest_ts": "Linaje: fecha-hora de la corrida (batch) o del micro-lote (streaming).",
    "ingest_date": "Fecha de ingesta; clave de partición de Bronze (D-005, D-011).",
    "silver_ts": "Linaje: fecha-hora de la corrida que escribió Silver.",
    "quality_flags": "Reglas de calidad marcadas en la fila (no bloqueantes).",
    "source": "Ruta que produjo el valor: streaming (provisorio) o reconciliacion.",
    "last_reconciled_at": "Fecha-hora de la última reconciliación del mart (§6.7).",
    "processed_at": "Fecha-hora en que el micro-lote escribió la fila.",
    "batch_id": "Micro-lote de Structured Streaming que escribió la fila.",
    "loaded_at": "Fecha-hora de carga en Cassandra.",
    # mart FinOps
    "usage_date": "Día de uso; clave de partición de Gold.",
    "daily_cost_usd": "Costo neto del día en USD (incluye créditos).",
    "daily_cost_gross_usd": "Costo bruto del día en USD (solo positivos).",
    "credit_adjustment_usd": "Suma de créditos del día en USD (negativo o cero).",
    "event_count": "Cantidad de eventos agregados.",
    "events_metric_null": "Eventos con métrica nula cuyo costo sí se agregó (D-009).",
    # anomalías
    "date": "Día de uso de la celda (org, día, servicio).",
    "baseline_median_usd": "Mediana del costo diario bruto de la línea de base.",
    "baseline_mad_usd": "MAD del costo diario bruto de la línea de base.",
    "baseline_source": "Línea de base usada: serie, servicio o sin_dispersion (D-016).",
    "history_days": "Días de historia de la serie (org, servicio).",
    "anomaly_score": "Score robusto: 0,6745 · (costo - mediana) / MAD.",
    "is_anomaly": "Anomalía si |score| > 3,5 (D-010).",
    "direction": "pico o caida; nulo si no es anomalía.",
    "has_credit_adjustment": "El día tuvo créditos (se reportan aparte, no compiten).",
    "method": "Método y umbral aplicados.",
    # quarantine
    "entity": "Dataset de origen del registro en quarantine.",
    "regla": "Regla de calidad violada (motivo).",
    "detalle": "Descripción legible de la violación.",
    "record_json": "Registro completo serializado a JSON (lo que se pudo parsear).",
    "raw_record": "Línea cruda cuando no se pudo parsear; nulo en otro caso.",
    "destino_en_silver": "promovido_con_flag (sigue en Silver) o descartado (duplicado).",
    # serving P2
    "as_of_date": "Fecha de corte de la ventana de 14 días.",
    "rank": "Posición del servicio por costo en la ventana (1 = más caro).",
    "cost_14d_usd": "Costo neto acumulado en los 14 días, en USD.",
    "requests_14d": "Requests acumulados en los 14 días.",
    "days_with_usage": "Días con uso del servicio dentro de la ventana.",
    "window_start": "Primer día de la ventana de 14 días.",
}

R1, R2, R3, R4 = ("R1 Arquitectura y gobierno", "R2 Ingesta y calidad",
                  "R3 Procesamiento y analítica", "R4 Serving y reproducibilidad")

_BATCH = ["customers_orgs", "users", "resources", "support_tickets", "marketing_touches",
          "nps_surveys", "billing_monthly"]


def _ds(zone, path, grain, owner, produced_by, sources, partition, retention, purpose,
        overrides=None):
    return {"zone": zone, "path": path, "grain": grain, "owner": owner,
            "produced_by": produced_by, "sources": sources, "partition": partition,
            "retention": retention, "purpose": purpose, "overrides": overrides or {}}


DATASETS = {}
for _e in _BATCH:
    DATASETS[f"bronze.{_e}"] = _ds(
        "Bronze", f"bronze/{_e}", "el de la fuente", R2, "src.ingest.batch_to_bronze",
        [f"landing/{_e}.csv"], "ingest_date", "24 meses",
        f"Copia tipada y deduplicada de {_e}.csv, una foto por día de ingesta.")
    DATASETS[f"quarantine.{_e}"] = _ds(
        "Quarantine", f"quarantine/{_e}", "un registro rechazado", R2,
        "src.ingest.batch_to_bronze", [f"landing/{_e}.csv"], "ingest_date, regla", "6 meses",
        "Filas no parseables, con clave nula o repetida.")
DATASETS.update({
    "bronze.usage_events": _ds(
        "Bronze", "bronze/usage_events", "un evento", R2, "src.ingest.stream_to_bronze",
        ["landing/usage_events_stream/*.jsonl"], "ingest_date", "24 meses",
        "Eventos de uso tipados y deduplicados por event_id (D-013)."),
    "quarantine.usage_events": _ds(
        "Quarantine", "quarantine/usage_events", "un evento rechazado", R2,
        "src.ingest.stream_to_bronze", ["landing/usage_events_stream/*.jsonl"],
        "ingest_date, regla", "6 meses", "JSON no parseable, event_id nulo o timestamp inválido."),
    "silver.customers_orgs": _ds(
        "Silver", "silver/customers_orgs", "una organización", R3, "src.transform.silver_job",
        ["bronze.customers_orgs", "bronze.nps_surveys"], "sin particionar", "24 meses",
        "Dimensión de organizaciones con el NPS resuelto (D-006).",
        {"nps_score": "NPS para consumo: última encuesta, si no CRM (D-006)."}),
    "silver.resources": _ds(
        "Silver", "silver/resources", "un recurso", R3, "src.transform.silver_job",
        ["bronze.resources"], "sin particionar", "24 meses", "Recursos con etiquetas parseadas."),
    "silver.usage_events": _ds(
        "Silver", "silver/usage_events", "un evento", R3, "src.transform.silver_job",
        ["bronze.usage_events", "silver.customers_orgs"], "event_date, service", "24 meses",
        "Eventos conformados, v1/v2 compatibles, con features y flags de calidad."),
    "quarantine.silver_usage_events": _ds(
        "Quarantine", "quarantine/silver_usage_events", "una regla violada por un evento", R2,
        "src.transform.silver_job", ["bronze.usage_events"], "regla", "6 meses",
        "Copias de auditoría de eventos marcados (siguen en Silver)."),
    "gold.org_daily_usage_by_service": _ds(
        "Gold", "gold/org_daily_usage_by_service", "org_id, usage_date, service", R3,
        "src.transform.gold_job", ["silver.usage_events"], "usage_date", "36 meses",
        "Mart FinOps oficial (reconciliado). Responde P1 y P2."),
    "gold.org_daily_usage_by_service_speed": _ds(
        "Gold", "gold/org_daily_usage_by_service_speed", "clave del mart × micro-lote", R3,
        "src.transform.gold_stream", ["bronze.usage_events", "silver.customers_orgs"],
        "batch_id", "vida del job",
        "Capa rápida: valores provisorios del streaming, sin tardíos (D-015)."),
    "gold.cost_anomaly_mart": _ds(
        "Gold", "gold/cost_anomaly_mart", "org_id, date, service", R3, "src.analytics.anomalies",
        ["gold.org_daily_usage_by_service"], "date", "36 meses",
        "Score y flag de anomalía de costo por celda (D-010, D-016). Responde P6."),
    "cassandra.org_daily_usage_by_service": _ds(
        "Serving", "cloud_analytics.org_daily_usage_by_service", "org_id, usage_date, service", R4,
        "src.serving.load_cassandra", ["gold.org_daily_usage_by_service",
                                       "gold.org_daily_usage_by_service_speed"],
        "PK ((org_id), usage_date, service)", "la del mart", "Tabla de serving de P1 (D-017)."),
    "cassandra.top_services_by_org_14d": _ds(
        "Serving", "cloud_analytics.top_services_by_org_14d", "org_id, as_of_date, rank", R4,
        "src.serving.load_cassandra", ["gold.org_daily_usage_by_service"],
        "PK ((org_id, as_of_date), rank)", "la del mart", "Tabla de serving de P2 (D-017)."),
})


def describe(dataset: str, column: str):
    """Descripción de una columna en un dataset, o None si no está documentada."""
    return DATASETS[dataset]["overrides"].get(column) or COLUMN_DOCS.get(column)
