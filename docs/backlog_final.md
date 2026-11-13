# Backlog final · MVP integrado (07/12/2026, 21:30)

Consigna §6.2 y §6.6: priorizar el trabajo restante en obligatorio, deseable y
fuera de alcance. **Regla de congelamiento:** lo deseable se empieza solo con lo
obligatorio del mismo eje terminado, y nada deseable se toca después del **30/11**.
Desde esa fecha, solo integración, pruebas y defensa.

Base: checklist 9.3 de la consigna, consultas obligatorias §7.4, marts §7.3 y lo
comprometido en el diseño del TP1. Los responsables son los roles del TP1 (§12.1).
Las horas son horas-persona; el TP1 estimó 102 h para esta etapa.

## Obligatorio

| # | Ítem | Requisito | Resp. | h | Evidencia de cierre |
|---|---|---|---|---|---|
| O-1 | Mart `revenue_by_org_month` (P4): Silver de billing con FX = 1,0 en USD y `credits` nulo = 0 (D-007), Gold mensual | §7.3, consulta 4 | R3 | 8 | Revenue total y por factura contra un cálculo independiente |
| O-2 | Mart `tickets_by_org_date` (P3): conteos por severidad, tasa de incumplimiento de SLA, CSAT promedio; nulos semánticos sin quarantine | §7.3, consulta 3 | R3 | 6 | Totales cuadran con Bronze (1.000 tickets, 95 SLA, 56 críticos) |
| O-3 | Mart `genai_tokens_by_org_date` (P5): tokens y costo estimado; solo v2 y genai | §7.3, consulta 5 | R3 | 4 | Tokens totales = suma en Silver |
| O-4 | Tablas query-first y consultas Q3, Q4 y Q5 en AstraDB, con CQL, justificación y captura | §7.4 | R4 | 8 | `evidence/serving/` con las 5 consultas, < 1 s cada una |
| O-5 | Invariantes entre columnas del TP1 (§9.7): `last_login >= created_at` (232) y `converted ⟹ clicked` (96) como flags en Silver de `users` y `marketing_touches` | §7.2 punto 7; coherencia con el diseño | R2 | 3 | Conteos iguales a los del perfilado |
| O-6 | Cerrar A-09: el componente analítico es `cost_anomaly_mart`, con métricas y límites (D-016). P7 pasa a deseable | §7.2 punto 6 | R3 | 2 | Decisión registrada; sección en la documentación |
| O-7 | Extender gobierno a los marts nuevos: catálogo, diccionario, controles de conservación, tests e idempotencia | §7.2 puntos 7 y 8 | R1 / R2 | 5 | `test_governance` en verde; doble corrida con variación 0 |
| O-8 | Notebook de Colab y `src.pipeline` con los 5 marts y las 5 consultas, validados desde entorno limpio | §7.2 punto 10, O7 | R4 | 4 | Corrida limpia en Colab con evidencias |
| O-9 | Diagrama v3 final y documentación técnica: arquitectura, modelo de serving, calidad, gobierno, linaje y limitaciones | §7.5 | R1 | 10 | `docs/` consistente con el código |
| O-10 | Incorporar el feedback de la 2.ª entrega (16/11) como plan de correcciones | §6.6 | R1 | 4 | `docs/plan_correcciones.md` actualizado |
| O-11 | Presentación ejecutiva | §7.5 | los 4 | 10 | Deck |
| O-12 | Video explicativo: pipeline, resultados y consultas | §7.5 | los 4 | 8 | Video |
| O-13 | Defensa: guion, demo y plan B si AstraDB falla (evidencias y capturas, riesgo G-02) | §7.2 punto 10, §8.4 | los 4 | 8 | Ensayo con tiempo medido |
| | **Subtotal obligatorio** | | | **80** | |

## Deseable

En orden de prioridad. Se toma de arriba hacia abajo mientras sobre tiempo antes del 30/11.

| # | Ítem | Valor | Resp. | h |
|---|---|---|---|---|
| D-1 | Token de AstraDB de solo lectura para el consumo (mínimo privilegio) | Seguridad, declarado en `docs/gobierno.md` | R4 | 1 |
| D-2 | `cost_anomaly_mart` en AstraDB (P6) | Serving de las anomalías | R4 | 3 |
| D-3 | MAD con ventana móvil en lugar de la serie completa | Detección más realista en producción (D-016) | R3 | 4 |
| D-4 | Reconciliación incremental: recomputar solo las fechas que tocaron los lotes nuevos | Escala (D-005, §12.6 del TP1) | R3 | 5 |
| D-5 | P7: relación entre experiencia de soporte y riesgo de abandono | Pregunta del TP1, con señal débil: 20 orgs sin NPS, 25% de tickets sin CSAT | R3 | 8 |
| D-6 | SCD tipo 2 para `customers_orgs` | Historia de cambios de plan y etapa | R3 | 4 |
| | **Subtotal deseable** | | | **25** |

## Fuera de alcance

Declarado desde el TP1 (§2.5, §12.6) o surgido en esta entrega. Se documenta como próximo paso, no se implementa.

| Ítem | Motivo |
|---|---|
| Orquestación con Airflow | `src.pipeline` alcanza para la demo; la cadencia es manual |
| Lakehouse (Delta o Iceberg) | Resolvería upserts y time travel, pero cambia el formato pedido (Parquet) |
| Clúster real (YARN, Kubernetes) y almacenamiento de objetos | El entorno es Colab `local[*]` (S-01) |
| Herramienta de visualización propia | El serving se expone para un BI externo |
| Anomalías con estacionalidad | 4 meses de datos no alcanzan para calibrarla |
| Ejecución de la retención por zona | Declarada; no se puede ejercitar con 4 meses |
| Conector JVM de Spark para Cassandra | El driver de Python cubre el caso (D-017) |

## Calendario

| Semana | Foco |
|---|---|
| 17–23/11 | O-10, O-1, O-2, O-3, O-5 |
| 24–30/11 | O-4, O-6, O-7, O-8 · deseables D-1 a D-3 si sobra tiempo · **30/11: congelamiento** |
| 01–07/12 | O-9, O-11, O-12, O-13 · ensayo de la defensa · entrega el 07/12 a las 21:30 |
