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
