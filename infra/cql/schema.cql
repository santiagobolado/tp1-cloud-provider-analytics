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
