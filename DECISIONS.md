# Registro de decisiones técnicas

Formato: contexto, decisión, alternativas descartadas, consecuencias.

---

## D-001 · Cardinalidad exacta en el perfilado

**Contexto.** `approx_count_distinct` de Spark opera sobre HyperLogLog. A la escala
de este dataset (decenas a miles de filas) el error es visible: reportaba 83
`org_id` distintos sobre 80 filas, lo que invalidaba la lectura del perfil.

**Decisión.** Usar `count_distinct` (exacto) en el perfilado exploratorio.

**Alternativa descartada.** Mantener la versión aproximada por performance.
No se justifica: el costo es irrelevante a este volumen y el error compromete
la evidencia que sustenta la §4.

**Consecuencia.** La decisión se revierte cuando el volumen lo exija. En el
pipeline productivo sobre eventos, la aproximación vuelve a ser preferible.

---

## D-002 · Los rangos válidos se derivan del dominio observado

**Contexto.** La primera pasada del perfilado evaluó `nps_score` contra la escala
0–10 de una respuesta individual de encuesta y marcó 59 de 80 registros como
anómalos. La distribución real (mín. −38, máx. 101, mediana 14) corresponde a la
métrica NPS agregada, cuya escala es −100 a +100. Con la regla correcta hay una
sola violación.

**Decisión.** Ninguna regla de calidad se escribe contra un rango asumido. Se
deriva de la distribución observada y se contrasta con la semántica del negocio
antes de fijarla.

**Consecuencia.** Aplicado a `genai_tokens`: la condición de presencia es
`schema_version = 2 AND service = 'genai'`, no solo la versión. La regla ingenua
habría producido 251 falsos positivos sobre 275 eventos v2.

---

## D-003 · `value` se lee como String en la fuente de eventos

**Contexto.** La fuente emite `value` indistintamente como número o como texto.
Un esquema que lo declare `Double` descarta silenciosamente las variantes texto.

**Decisión.** Declararlo `String` en Bronze y castear en Silver con fallback
controlado, enviando el fallo a quarantine en lugar de nulificarlo.

**Consecuencia.** Bronze conserva el valor tal como llegó, preservando
trazabilidad al dato crudo.

---

## D-004 · Patrón híbrido segmentado con reconciliación acotada

**Contexto.** Los tres patrones admitidos son Lambda, Kappa e híbrido. Las fuentes
tienen latencias de negocio incompatibles: eventos continuos frente a facturación
que se consolida una vez por mes (3 valores de `month` en 240 filas).

**Decisión.** Híbrido en dos componentes: (1) segmentación de rutas por fuente
según la latencia que su decisión de negocio exige; (2) un job batch de
reconciliación que recomputa los marts de eventos desde Bronze.

**Alternativas descartadas.**
- *Kappa*: modelar maestros y facturación como streams agrega checkpoints y estado
  sin reducir ninguna latencia relevante. Además depende de un log persistente que
  el entorno declarado (S-01) no ofrece.
- *Lambda canónico*: procesa los mismos datos por dos caminos y los fusiona en la
  consulta. Acá las rutas procesan fuentes distintas, no hay vista duplicada del
  mismo dato. Llamarlo Lambda sería impreciso.

**Fundamento del componente 2.** Los archivos de eventos cubren 59 días sin orden
cronológico. Un watermark corto —necesario para O1— descarta eventos tardíos, con
lo que el streaming produce agregados incompletos por naturaleza de la fuente. La
recomputación batch desde Bronze los corrige.

**Consecuencias.** Lógica duplicada acotada a los marts de eventos (se mitiga
factorizando la transformación compartida); ventana de inconsistencia entre
corridas (se mitiga exponiendo la marca de última reconciliación en el mart); dos
rutas a mantener.

**Pendiente de verificación.** El perfilado se hizo sobre un solo archivo. Si el
dataset completo mostrara eventos ordenados, la reconciliación dejaría de ser
necesaria.

> **Verificado en D-012 (2.ª entrega):** los 120 archivos no tienen orden temporal; la
> reconciliación es necesaria.

---

## D-005 · Bronze particiona por fecha de ingesta; Silver y Gold por fecha de evento

**Contexto.** Los archivos de eventos cubren 59 días sin orden cronológico. Con
Bronze particionado por fecha de evento, cada micro-lote escribiría en casi todas
las particiones del histórico: small files en su forma más aguda.

**Decisión.** Bronze de eventos particiona por `ingest_date` (una partición por
micro-lote); Silver y Gold por `event_date`, con `service` como segunda clave.
Los maestros no se particionan: 80 a 1.500 filas.

**Alternativa descartada.** Particionar Bronze por fecha de evento y compactar
después. Agrega un job de compactación para resolver un problema que el cambio de
eje evita.

**Consecuencia positiva.** El job de reconciliación (D-004) puede leer solo las
particiones de Bronze con `ingest_date` reciente para identificar qué fechas de
evento tocaron los datos tardíos, y recomputar únicamente esas particiones de Gold.

**Limitación.** A escala de la muestra (~120 eventos diarios) el esquema está
sobre-particionado. Se compensa con `coalesce(1)` al escribir. El esquema se
justifica contra el caso dimensionado en la §3.2.

---

## D-006 · `nps_surveys` es la fuente de verdad del NPS

**Contexto.** El NPS aparece en `customers_orgs.nps_score` (80 orgs, 11 nulos) y
en `nps_surveys.csv` (92 encuestas, 60 orgs, 32 con más de una), con valores que
no coinciden.

**Decisión.** `nps_surveys` es la fuente de verdad por ser el hecho con fecha.
Silver conserva **ambos** valores con nombres distintos (`nps_score_crm`,
`nps_score_last_survey`, `nps_survey_date`) en lugar de sobrescribir uno con otro.

**Fundamento del caso borde.** 20 de 80 organizaciones nunca fueron encuestadas.
Eliminar la columna del CRM las dejaría sin NPS. La regla de consumo es usar la
última encuesta con fallback al CRM, marcando el origen.

---

## D-007 · La tasa de cambio se fuerza a 1,0 para facturas en USD

**Contexto.** Las tasas de ARS (mediana 0,00150) y EUR (mediana 1,10467) son
plausibles. Las 160 filas en USD tienen tasas entre 0,85463 y 1,11791 en lugar de
1,0: ruido alrededor de 1.

**Decisión.** `amount_usd = neto` si `currency = 'USD'`, y `neto × fx` en otro
caso. La fila se marca con `fx_usd_ruidoso` en `quality_flags`; no va a
quarantine porque el dato de negocio es recuperable.

**Fundamento.** En el agregado el ruido se compensa: la diferencia de revenue
total es del 0,1% y una validación global no lo detectaría. Pero por factura la
distorsión llega a −15% / +12%, y los marts reportan por organización y mes. Un
error que se cancela en el total sigue siendo un error en cada fila.

**Decisión asociada.** `credits` nulo (137 de 240 filas) significa ausencia de
crédito y se resuelve como 0 antes de restar, para no anular el revenue neto de
más de la mitad del dataset.

---

## D-008 · Watermark de 7 días sobre el event-time de los eventos

**Contexto.** Cada archivo cubre el rango completo del dataset, con lo que el
primer micro-lote ya lleva el máximo event-time a 2025-08-31. Desde el segundo en
adelante, todo lo anterior al umbral se descarta de la agregación.

**Cuantificación sobre el archivo perfilado**: un watermark de 7 días procesa el
8,9% de los eventos; uno de 30 días, el 51,9%; uno de 60 días, el 100%.

**Decisión.** 7 días.

**Fundamento.** El watermark se dimensiona contra el caso productivo, no contra la
muestra. El estado de deduplicación crece con la ventana: a 3,6 millones de
eventos diarios, 7 días implican ~25 millones de claves (cientos de MB de estado);
60 días implicarían más de 200 millones, inviable en un solo nodo.

**Lo que implica aceptar.** La ruta de streaming no produce marts completos y no
se pretende que lo haga: aporta frescura sobre eventos recientes (O1). La
completitud la aporta el flujo D de reconciliación.

**Es la validación empírica de D-004**: con estos datos, la reconciliación no es
una precaución teórica sino lo único que hace correctos a los marts.

**Sujeto a A-07.** Si los ~20 archivos mostraran eventos ordenados, el porcentaje
descartado caería y el watermark podría reducirse.

> **Verificado en D-012 (2.ª entrega):** con los 120 archivos, 7 días descarta el 87,6%
> (simulado) y el 85,2% (medido en la agregación, D-015). Se mantiene 7 días.

---

## D-009 · El rechazo por calidad opera sobre el campo derivado, no sobre el registro

**Contexto.** Al trazar el job de MapReduce con registros reales apareció una
tensión con la regla escrita en la §9.7. Un evento con `value` nulo tiene la
métrica inutilizable, pero su `cost_usd_increment` es perfectamente válido.
Enviar el registro completo a quarantine perdería ese costo.

**Cuantificación.** En el archivo perfilado, los 9 eventos con `value` nulo y los
17 con `unit` nulo arrastran **el 4,6% del costo total** (43,35 de 932,82 USD).

**Decisión.** El registro promueve a Silver con la métrica en nulo y marcada en
`quality_flags`; el costo participa del mart. Una copia del registro va a
quarantine para auditoría.

**Alternativa descartada.** Rechazar el registro entero. Es más simple de
implementar pero subestima silenciosamente el revenue de FinOps, que es
exactamente la clase de error que D-007 buscaba evitar.

**Consecuencia.** Quarantine deja de ser "lo que no entró" para ser "lo que entró
con reservas, con su motivo". El conteo de quarantine no es complementario al de
Silver.

---

## D-010 · MAD sobre la serie diaria por organización y servicio

**Contexto.** Los tres métodos admitidos se evaluaron sobre los datos reales.
Aplicados globalmente, MAD marca el 23,9% de los eventos, z-score el 1,7% y
percentiles el 1,9%.

**Diagnóstico.** La dispersión de MAD no es un error del método: la distribución
global es una mezcla de escalas. La mediana de costo de un evento de `requests`
(5,1741) es **87 veces** la de uno de `cpu_hours` (0,0594). Agrupar por servicio
no alcanza: cada servicio mezcla métricas y sus medianas quedan parejas (0,46 a
1,23).

**Decisión.** La detección opera sobre el mart diario
`org_daily_usage_by_service` —donde la mezcla ya está colapsada en un
`daily_cost_usd` por celda— con línea de base por serie `(org_id, service)` a lo
largo del tiempo, usando MAD y umbral `|score| > 3,5`.

**Por qué MAD.** Media y desvío se contaminan con los valores extremos que buscan
detectar. Mediana y MAD tienen punto de ruptura del 50%. Con media 2,59 contra
mediana 0,75, la asimetría lo hace material.

**Por qué no percentiles.** Un corte en p99 marca siempre el 1%, haya o no
anomalías: es una cuota, no una detección.

**Viabilidad verificada.** 181 series `(org, service)`; extrapoladas a los ~20
archivos, todas superarían los 20 días de historia. Con menos de 20 días se cae a
la estadística por servicio, marcando el origen de la línea de base.

**Decisión asociada.** Los costos negativos se marcan como
`is_credit_adjustment` y no participan de la detección de picos: son un fenómeno
de negocio distinto y la sesgarían.

**Límite reconocido.** Con 4 meses no hay estacionalidad observable. Un método con
descomposición de tendencia y estacionalidad sería superior pero no se puede
calibrar con esta ventana.

---

## D-011 · Los maestros batch particionan por `ingest_date` y se reescriben por día

**Fecha**: 2026-11-11 · **Estado**: aceptada · **Ámbito**: ingesta batch · **Precisa**: D-005

**Contexto.** D-005 dejó los maestros "sin particionar o por `ingest_date`" por su
volumen (80 a 1.500 filas). La consigna (§6.2) exige Bronze en "Parquet
particionado". Además había que definir cómo se re-ejecuta sin duplicar.

**Decisión.** Las siete fuentes batch particionan por `ingest_date` con
`coalesce(1)`: un archivo por fuente y por corrida. La escritura es
`overwrite` con `partitionOverwriteMode=dynamic`: cada corrida reemplaza solo
su propia partición. Bronze acumula una foto por día; Silver lee la más reciente.

**Alternativas descartadas.**
- *`append`*: cada re-ejecución duplica la carga del día. Viola O4.
- *Overwrite total de la tabla*: idempotente, pero borra el historial de
  ingestas, que es justamente lo que Bronze tiene que conservar (auditoría y
  reprocesamiento).

**Consecuencias.** Re-ejecutar el mismo día da el mismo resultado (verificado:
mismos conteos y un archivo por partición). Quarantine reescribe el día completo
de la entidad en lugar de partición por regla, porque el overwrite dinámico
dejaría huérfana una regla que tuvo rechazos en una corrida anterior y no en la
actual.

**Nota sobre la deduplicación.** El TP1 (§4.3, R8) citó 17/61/9 claves
duplicadas en tickets, marketing y billing. Esas cifras salieron de
`approx_count_distinct` antes de D-001; con conteo exacto hay **cero**
duplicados en las siete fuentes. La deduplicación se mantiene, pero su
fundamento no es la evidencia del dataset: es la garantía de idempotencia ante
re-entregas de la misma fuente. Si aparece una clave repetida, se conserva la
primera aparición en el archivo y las siguientes van a quarantine (`clave_duplicada`).
(Corregido en la auditoría del 13/11: la versión inicial ordenaba por contenido,
lo que podía conservar la copia y aislar el original. Lo cubre un test.)

---

## D-012 · Los archivos de eventos no tienen orden temporal: se confirma la reconciliación

**Fecha**: 2026-11-11 · **Estado**: aceptada · **Cierra**: A-07 · **Confirma**: D-004, D-008

**Contexto.** D-004 y D-008 se apoyaron en un solo archivo. El dataset completo
tiene **120 archivos y 43.200 eventos** (el TP1 estimó ~20 archivos y ~7.200).

**Evidencia** (`notebooks/02_perfilado_eventos_a07.py` → `evidence/a07_orden_eventos.md`):
- Los 120 archivos cubren entre 59 y 60 días cada uno: todos son una muestra de
  la ventana completa, ninguno es un corte cronológico.
- Correlación entre número de archivo y mediana de su timestamp: −0,015.
- Procesando un archivo por micro-lote, un watermark de 7 días descarta el
  **87,6%** de los eventos; uno de 30 días, el 49,6%; solo 60 días no descarta nada.
- Cero `event_id` repetidos, dentro o entre archivos.

**Decisión.** Se mantienen D-004 (híbrido con reconciliación) y D-008 (watermark de
7 días). La premisa que A-07 ponía en duda se verifica sobre el 100% del dataset:
la ruta streaming no puede producir marts completos y la reconciliación es lo
único que los hace correctos.

**Consecuencia.** La deduplicación por `event_id` sigue siendo obligatoria
(consigna §6.2) pero el dataset no la ejercita: se demuestra re-entregando un
archivo ya procesado. Las cifras de volumen del TP1 (§3.2) se corrigen en el
documento de la 2.ª entrega.

---

## D-013 · Dos watermarks: dedupe en Bronze sobre `ingest_ts`, agregación sobre `event_ts`

**Fecha**: 2026-11-11 · **Estado**: aceptada · **Ámbito**: streaming · **Precisa**: D-008

**Contexto.** La deduplicación con watermark de Structured Streaming descarta, sin
dejar rastro, todo evento cuyo tiempo quede por detrás del watermark. Con el
watermark de D-008 (7 días sobre `event_ts`) eso es el 87,6% del dataset (D-012).
Aplicado al escribir Bronze, la reconciliación de D-004 se quedaría sin insumo.

**Decisión.** Dos watermarks, cada uno en la operación donde tiene sentido:
- **Bronze**: `withWatermark("ingest_ts", "7 days").dropDuplicatesWithinWatermark(["event_id"])`.
  `ingest_ts` es la hora del micro-lote y siempre avanza, así que ningún evento
  queda detrás del watermark: Bronze conserva el 100% y descarta solo las
  re-entregas del mismo `event_id` dentro de 7 días de llegada.
- **Agregación del mart** (`org_daily_usage_by_service` en streaming): 7 días sobre
  `event_ts`, como fija D-008. Los tardíos quedan fuera del valor fresco, se
  cuentan (`numRowsDroppedByWatermark`) y los recupera la reconciliación desde Bronze.

**Alternativas descartadas.**
- *Watermark de 7 días sobre `event_ts` en Bronze*: pierde el 87,6% de los eventos.
- *Watermark de 60 días sobre `event_ts`*: no pierde nada en la muestra, pero
  contradice el dimensionamiento de D-008 (más de 200 millones de claves en estado
  en el escenario mediano).

**Verificado.** 120 micro-lotes de 360 eventos, 43.200 filas en Bronze, 0
descartadas por watermark. Re-entregar un archivo con otro nombre: se lee y no
se escribe. Re-ejecutar sin archivos nuevos: 0 filas procesadas.

**Límite reconocido.** Una re-entrega que llegue más de 7 días después de la
original no se deduplica en Bronze. Silver deduplica por `event_id` sobre la
tabla completa, lo que cubre ese caso.

**Hallazgo de la prueba.** En Spark 4 (modo ANSI) `to_timestamp` sobre un valor
inválido lanza una excepción y detiene la consulta. Se usa `try_to_timestamp`, que
devuelve nulo, y la regla `timestamp_invalido` envía el evento a quarantine.

---

## D-014 · El `unit` nulo se imputa desde la métrica; la métrica nula no se imputa

**Fecha**: 2026-11-12 · **Estado**: aceptada · **Ámbito**: Silver, calidad · **Refina**: D-009

**Contexto.** D-009 dejaba la métrica en nulo cuando faltaba `unit`. Con el dataset
completo son 2.038 eventos (4,7%): se perdería la métrica de todos ellos. El
perfilado de los 120 archivos muestra que la unidad es función de la métrica,
sin excepciones en los 41.125 eventos que la traen: `cpu_hours → hours`,
`requests → count`, `storage_gb_hours → gb_hours`.

**Decisión.** Cuando falta `unit` y hay `value`, se completa con la unidad canónica
de la métrica, el evento lleva el flag `unit_nulo_con_value` y una copia va a
quarantine. Es el mismo criterio que D-007: si el dato de negocio es recuperable
con certeza, se corrige y se deja auditable. Cuando falta `value` (877 eventos),
no hay nada que recuperar: la métrica queda en nulo y el costo promueve (D-009).

**Alternativa descartada.** Anular la métrica, como decía D-009. Descarta
información que el propio dataset permite reconstruir sin ambigüedad.

**Consecuencia.** Quarantine de Silver contiene copias de auditoría, no registros
excluidos: los 877 + 2.038 eventos están en Silver con su flag. Los 211 costos
negativos se marcan (`is_credit_adjustment`) pero no van a quarantine, porque no
son un defecto (§8.7). Los eventos con métrica nula llevan USD 2.968 de costo
(2,0% del total), que gracias a D-009 llegan al mart.

---

## D-015 · Gold con dos salidas: capa rápida (streaming) y mart reconciliado (batch)

**Fecha**: 2026-11-12 · **Estado**: aceptada · **Ámbito**: Gold · **Implementa**: D-004, D-008

**Decisión.** `org_daily_usage_by_service` se calcula por dos rutas con la misma
lista de agregaciones (`src/transform/gold.py::daily_usage_aggs`):

- **Flujo C (streaming)**: Bronze como stream, un archivo por micro-lote,
  `withWatermark("event_ts", "7 days")`, ventana diaria, `outputMode("update")`,
  `foreachBatch` a `gold/org_daily_usage_by_service_speed` particionado por
  `batch_id`. Reintentar un lote reescribe su partición: no duplica. El valor
  vigente por clave es el del último `batch_id`.
- **Flujo D (reconciliación)**: Silver completo → mart oficial en
  `gold/org_daily_usage_by_service`, por `usage_date`, con `last_reconciled_at`.

**Resultado con el dataset completo.**
- Reconciliado: 11,050 filas, 80 orgs, 60 días, USD 147,433.98; cuadra con Silver.
- Streaming: 120 micro-lotes; **36,808 eventos
  tardíos (85.2%)** quedan fuera de la capa rápida, que cubre el
  15.1% del costo. Latencia por micro-lote: mediana
  969 ms, máximo 6946 ms (O1: ≤ 5 min).
- Con un solo micro-lote las dos rutas dan exactamente el mismo mart (test): no
  divergen (riesgo G-04).

**Medición de tardíos.** Se calcula como eventos en Bronze − eventos incorporados a
la capa rápida (suma de `event_count` en la vista vigente). La métrica
`numRowsDroppedByWatermark` de Spark no sirve: los tardíos se filtran antes del
operador de estado y los subcuenta.

**Comportamiento de Spark 4 a declarar.** El filtro de tardíos usa el watermark
del lote anterior (Spark separa el watermark de tardíos del de desalojo de
estado para soportar varios operadores con estado). Los descartes empiezan en el
tercer micro-lote; por eso el 85.2% medido es algo menor que el 87,6% simulado en D-012.

**Alternativa descartada.** Que el streaming escriba directo sobre el mart
reconciliado: en Parquet no hay upsert, y sobrescribir una partición con el
parcial del streaming haría retroceder un valor ya consolidado. La fusión de las
dos rutas se hace en el serving: Cassandra hace upsert por clave primaria, el
streaming escribe primero y la reconciliación corrige después.

---

## D-016 · Implementación de D-010: línea de base sobre la serie completa

**Fecha**: 2026-11-12 · **Estado**: aceptada · **Ámbito**: analítica · **Implementa**: D-010

**Decisión.** `cost_anomaly_mart` (`src/analytics/anomalies.py`) aplica MAD sobre el
costo diario bruto de cada serie `(org_id, service)` del mart reconciliado, con
umbral `|score| > 3,5`. La línea de base usa la serie completa, no una ventana
móvil. Fallback a la estadística del servicio si la serie tiene menos de 20 días
o MAD = 0; si tampoco hay dispersión ahí, `baseline_source = sin_dispersion` y no
se marca.

**Por qué serie completa.** Con 60 días el resultado no cambia de forma
material, y Spark no calcula medianas sobre ventanas móviles sin un self-join. La
ventana móvil queda en el backlog para producción.

**Resultado.** 262 series, todas con ≥ 24 días de historia (el fallback no se
activa en este dataset). 130 anomalías en 11,050 celdas (1.18%).

**Validación sin etiquetas.** El dataset inyecta picos (eventos > USD 50 con
mediana de USD 1). Sobre el mismo mart:

| Método | Celdas marcadas | Picos detectados (de 54) |
|---|---|---|
| z-score global, \|z\| > 3 | 93 | 51 |
| MAD global, \|score\| > 3,5 | 405 | 54 |
| **MAD por serie (elegido)** | **130** | **54** |

El z-score global pierde picos en servicios baratos; el MAD global los detecta
pero marca tres veces más celdas, porque confunde "caro" con "anómalo". La línea
de base por serie es la única que detecta todo con el menor ruido.

**Límite.** Sin etiquetas no hay precisión medible: las 76 celdas marcadas que no
contienen un pico inyectado pueden ser anomalías reales o falsos positivos.

---

## D-017 · Modelo de serving: dos tablas query-first y carga con el driver de Python

**Fecha**: 2026-11-12 · **Estado**: aceptada · **Ámbito**: serving · **Cierra**: A-08 · **Revisa**: §8.9 del TP1

**Tablas** (`infra/cql/schema.cql`, keyspace `cloud_analytics`):

| Tabla | Clave | Responde |
|---|---|---|
| `org_daily_usage_by_service` | `((org_id), usage_date, service)`, `usage_date DESC` | P1: costo y requests por día y servicio en un rango de fechas |
| `top_services_by_org_14d` | `((org_id, as_of_date), rank)` | P2: Top-N servicios por costo en los 14 días previos a una fecha |

**Cambios respecto del TP1 (§8.9) y por qué.**
- P1 pasa de partición `(org_id, usage_date)` a `(org_id)` con `usage_date` como
  clustering. Con la fecha en la partición, un rango de fechas obliga a una
  consulta por día o a `IN`; como clustering, el rango es un corte contiguo de una
  sola partición. Tamaño: 360 filas por organización con este dataset; a escala
  se agregaría un bucket mensual a la partición.
- P2 ordena por `rank` y no por `total_cost DESC`. Si el costo fuera parte de la
  clave, recalcularlo crearía una fila nueva y dejaría la vieja: el upsert no
  sería idempotente. Con `rank`, la recarga pisa la misma fila.
- P2 se precalcula en Spark para cada fecha de corte con 14 días completos (47
  fechas, 12.314 filas): Cassandra no agrega ni ordena por columnas calculadas.

**Carga.** `cassandra-driver` en `foreachPartition` con sentencias preparadas. La
consigna admite "conector o foreachBatch/driver"; el Spark Cassandra Connector es
un JAR atado a versiones de Spark y Scala, sin compatibilidad asegurada con Spark
4.2. Todo INSERT es upsert: re-cargar no duplica. Con `--with-speed` se carga la
capa rápida y después el reconciliado la pisa (fusión de D-015).

**Entorno.** La carga y las consultas corren en Colab
(`notebooks/00_pipeline_colab.ipynb`): el entorno de desarrollo no tiene salida a
AstraDB. Credenciales solo por variables de entorno y Colab Secrets.

---

## Decisiones abiertas

| # | Tema | Bloquea | Fecha objetivo |
|---|---|---|---|
| A-09 | Alcance final del componente analítico o de ML | P7 | tras la 2.ª entrega |
