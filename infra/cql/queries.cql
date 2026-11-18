-- Las cinco consultas mínimas de la consigna (§7.4). Se ejecutan con
-- `python -m src.serving.run_queries`, que guarda CQL, filas y tiempos en evidence/.
-- También se pueden pegar en la CQL Console de AstraDB para la captura.
-- Organización y fechas de ejemplo: las del dataset (ventana 2025-07-03 a 2025-08-31).

-- Q1 (P1) · Costos y requests diarios por organización y servicio en un rango de fechas.
SELECT usage_date, service, daily_cost_usd, requests, event_count, source
FROM cloud_analytics.org_daily_usage_by_service
WHERE org_id = 'org_okep7y6w'
  AND usage_date >= '2025-08-01' AND usage_date <= '2025-08-07';

-- Q2 (P2) · Top-3 servicios por costo acumulado en los últimos 14 días.
SELECT rank, service, cost_14d_usd, requests_14d, window_start, as_of_date
FROM cloud_analytics.top_services_by_org_14d
WHERE org_id = 'org_okep7y6w' AND as_of_date = '2025-08-31'
LIMIT 3;

-- Q3 (P3) · Evolución de tickets críticos y tasa de SLA breach por día, últimos 30 días.
SELECT ticket_date, ticket_count, sla_breached_count, sla_breach_rate, open_count, csat_avg
FROM cloud_analytics.tickets_by_severity_date
WHERE severity = 'critical'
  AND ticket_date >= '2025-08-02' AND ticket_date <= '2025-08-31';

-- Q3b (P3) · La misma ventana para todas las severidades: tasa de SLA breach del soporte.
SELECT ticket_date, ticket_count, sla_breached_count, sla_breach_rate, csat_avg
FROM cloud_analytics.tickets_by_severity_date
WHERE severity = 'all'
  AND ticket_date >= '2025-08-02' AND ticket_date <= '2025-08-31';

-- Q4 (P4) · Revenue mensual de una organización, con créditos e impuestos, en USD.
SELECT month, currency, fx_applied, subtotal_usd, credits_usd, taxes_usd,
       net_revenue_usd, total_billed_usd
FROM cloud_analytics.revenue_by_org_month
WHERE org_id = 'org_okep7y6w';

-- Q4b (P4) · Revenue mensual de toda la plataforma.
SELECT month, org_count, invoice_count, currency, credits_usd, taxes_usd,
       net_revenue_usd, total_billed_usd
FROM cloud_analytics.revenue_by_org_month
WHERE org_id = 'all';

-- Q5 (P5) · Tokens GenAI y costo estimado por día de una organización (agosto).
SELECT usage_date, genai_tokens, genai_cost_usd, cost_per_1k_tokens_usd, event_count
FROM cloud_analytics.genai_tokens_by_org_date
WHERE org_id = 'org_okep7y6w'
  AND usage_date >= '2025-08-01' AND usage_date <= '2025-08-31';

-- Q5b (P5) · Tokens GenAI y costo estimado por día de toda la plataforma (últimos 14 días).
SELECT usage_date, org_count, genai_tokens, genai_cost_usd, cost_per_1k_tokens_usd
FROM cloud_analytics.genai_tokens_by_org_date
WHERE org_id = 'all'
  AND usage_date >= '2025-08-18' AND usage_date <= '2025-08-31';
