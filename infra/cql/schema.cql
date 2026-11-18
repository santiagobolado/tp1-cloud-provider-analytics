-- Serving query-first del mart FinOps org_daily_usage_by_service (D-017).
-- En AstraDB el keyspace se crea desde la consola web (no admite CREATE KEYSPACE
-- por CQL). Para Cassandra local, ver keyspace_local.cql.
-- La tabla se diseña desde la consulta, no desde la entidad: un mismo mart
-- origina dos tablas porque P1 y P2 necesitan órdenes distintos.

-- P1 · Costos y requests diarios por organización y servicio en un rango de fechas.
--   Partición: org_id      -> la consulta siempre trae una sola organización.
--   Clustering: usage_date -> el rango de fechas es un corte contiguo en disco;
--               service    -> completa la clave del grano (org, día, servicio).
--   Tamaño de partición: 60 días x 6 servicios = 360 filas por organización.
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
    org_id               text,
    usage_date           date,
    service              text,
    org_name             text,
    plan_tier            text,
    daily_cost_usd       double,
    daily_cost_gross_usd double,
    credit_adjustment_usd double,
    requests             double,
    cpu_hours            double,
    storage_gb_hours     double,
    genai_tokens         bigint,
    carbon_kg            double,
    event_count          bigint,
    source               text,        -- 'streaming' (provisorio) | 'reconciliacion'
    last_reconciled_at   timestamp,
    loaded_at            timestamp,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

-- P2 · Top-N servicios por costo acumulado en los últimos 14 días de una organización.
--   Cassandra no agrega ni ordena por una columna calculada: el Top-N se
--   precalcula en Spark para cada fecha de corte (as_of_date).
--   Clustering por rank y no por costo: si el costo fuera clave, recalcular
--   dejaría la fila vieja y la nueva (otra clave). Con rank, el upsert pisa.
CREATE TABLE IF NOT EXISTS top_services_by_org_14d (
    org_id          text,
    as_of_date      date,
    rank            int,
    service         text,
    cost_14d_usd    double,
    requests_14d    double,
    days_with_usage int,
    window_start    date,
    loaded_at       timestamp,
    PRIMARY KEY ((org_id, as_of_date), rank)
) WITH CLUSTERING ORDER BY (rank ASC);

-- P3 · Evolución de tickets críticos y tasa de SLA breach por día, últimos 30 días (D-018).
--   La consulta no menciona organización: la partición es la severidad.
--   Partición: severity     -> una sola partición responde la serie ('critical' o 'all').
--   Clustering: ticket_date -> los 30 días son un corte contiguo, del más reciente al más viejo.
--   'all' es una partición precalculada en Spark con todas las severidades: Cassandra
--   no agrega entre particiones. La serie viene completa: los días sin tickets están en 0.
--   Tamaño: 115 días por partición, 5 particiones.
CREATE TABLE IF NOT EXISTS tickets_by_severity_date (
    severity           text,
    ticket_date        date,
    ticket_count       int,
    open_count         int,
    sla_breached_count int,
    sla_breach_rate    double,
    csat_responses     int,
    csat_avg           double,
    org_count          int,
    loaded_at          timestamp,
    PRIMARY KEY ((severity), ticket_date)
) WITH CLUSTERING ORDER BY (ticket_date DESC);

-- P4 · Revenue mensual con créditos e impuestos aplicados, normalizado a USD (D-019, D-024).
--   Partición: org_id -> el revenue se consulta por cliente; org_id = 'all' es el total
--              de la plataforma, precalculado (misma idea que 'all' en P3).
--   Clustering: month -> los meses de una organización, del más reciente al más viejo.
--   Tamaño: 3 filas por partición con este dataset (12 por año).
CREATE TABLE IF NOT EXISTS revenue_by_org_month (
    org_id           text,
    month            date,
    org_name         text,
    plan_tier        text,
    currency         text,
    fx_applied       double,
    subtotal_usd     double,
    credits_usd      double,
    taxes_usd        double,
    net_revenue_usd  double,
    total_billed_usd double,
    invoice_count    int,
    has_credit_note  boolean,
    org_count        int,
    loaded_at        timestamp,
    PRIMARY KEY ((org_id), month)
) WITH CLUSTERING ORDER BY (month DESC);

-- P5 · Tokens GenAI y costo estimado por día, cuando existan (D-020, D-024).
--   Partición: org_id -> serie diaria de una organización; 'all' = total de la plataforma.
--   Clustering: usage_date -> rango de fechas como corte contiguo.
--   "Cuando existan": solo hay filas desde 2025-07-18 (eventos v2); una organización
--   sin uso de GenAI no tiene partición, y la consulta devuelve 0 filas.
--   Tamaño: ≤ 45 filas por partición con este dataset.
CREATE TABLE IF NOT EXISTS genai_tokens_by_org_date (
    org_id                 text,
    usage_date             date,
    org_name               text,
    plan_tier              text,
    genai_tokens           bigint,
    genai_cost_usd         double,
    cost_per_1k_tokens_usd double,
    event_count            int,
    org_count              int,
    loaded_at              timestamp,
    PRIMARY KEY ((org_id), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);
