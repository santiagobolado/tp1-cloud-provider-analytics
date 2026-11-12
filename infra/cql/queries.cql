-- Consultas mínimas de la 2.ª entrega (consigna §6.2). Se ejecutan con
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
