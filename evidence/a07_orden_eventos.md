# A-07 · Orden temporal de los archivos de eventos

Generado: 2026-11-12T00:03:09+00:00 · Spark 4.2.0 · `data/landing/usage_events_stream/`

**120 archivos · 43,200 eventos** · ventana 2025-07-03 00:02:00 → 2025-08-31 23:58:00

## 1. Cada archivo cubre casi toda la ventana

Span por archivo: mín. 59.0 días, máx. 60.0, promedio 59.7.
Correlación entre número de archivo y mediana de su timestamp: **-0.015** (≈ 0: el número de archivo no dice nada sobre la fecha de sus eventos).

| archivo | eventos | ts_min | ts_max | span (días) |
|---|---|---|---|---|
| 0000 | 360 | 2025-07-03 00:08:00 | 2025-08-31 16:32:00 | 59.7 |
| 0001 | 360 | 2025-07-03 02:38:00 | 2025-08-31 23:51:00 | 59.9 |
| 0002 | 360 | 2025-07-03 14:16:00 | 2025-08-31 20:47:00 | 59.3 |
| 0003 | 360 | 2025-07-03 00:48:00 | 2025-08-31 23:51:00 | 60.0 |
| 0004 | 360 | 2025-07-03 10:43:00 | 2025-08-31 20:40:00 | 59.4 |
| 0117 | 360 | 2025-07-03 01:03:00 | 2025-08-31 21:52:00 | 59.9 |
| 0118 | 360 | 2025-07-03 02:32:00 | 2025-08-31 04:48:00 | 59.1 |
| 0119 | 360 | 2025-07-03 00:26:00 | 2025-08-31 17:24:00 | 59.7 |

(primeros 5 y últimos 3; el detalle completo está en el JSON)

## 2. Descarte por watermark procesando un archivo por micro-lote

| watermark | descartados | % descartado | % procesado |
|---|---|---|---|
| 1 días | 42,118 | 97.5% | 2.5% |
| 2 días | 41,405 | 95.8% | 4.2% |
| 7 días | 37,831 | 87.6% | 12.4% |
| 14 días | 32,833 | 76.0% | 24.0% |
| 30 días | 21,421 | 49.6% | 50.4% |
| 60 días | 0 | 0.0% | 100.0% |

## 3. Duplicados de `event_id`

- Repetidos: **0** claves (0 filas sobrantes)
- Entre archivos distintos: **0** · dentro de un mismo archivo: **0**
- Con contenido distinto entre copias: **0**

## 4. Calidad del dataset completo

- `value_nulo`: **877**
- `value_no_casteable`: **0**
- `unit_nulo_con_value`: **2038**
- `cost_menor_a_-0.01`: **211**
- `cost_min`: **-154.4608**
- `cost_max`: **317.4308**
- `timestamp_no_parseable`: **0**
- `schema_version`: {'1': 10800, '2': 32400}
