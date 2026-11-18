# Cloud Provider Analytics

Proyecto integrador de **Big Data** · ITBA · 2.º cuatrimestre 2026
Pipeline de ETL, streaming y serving para analítica de FinOps, Soporte y Producto
sobre los datos de un proveedor de nube.

## Estructura

```
docs/        Documento de diseño, diagramas y material de defensa
data/        Landing (crudo, inmutable). No se versionan datos sensibles.
notebooks/   Exploración y perfilado
src/         Código de ingesta, procesamiento y serving
tests/       Pruebas de transformaciones y reglas de calidad
config/      Configuración externalizada (sin credenciales)
infra/       Scripts y manifiestos de ejecución
evidence/    Logs, salidas y capturas de cada entrega
DECISIONS.md Registro de decisiones técnicas y trade-offs
```

## Requisitos

- Python 3.10+ y Java 17 o superior (Spark 4 no corre con Java 11)
- Dependencias con versión fija en `requirements.txt`: PySpark 4.2.0, pytest,
  cassandra-driver 3.30.1
- **Entorno de referencia: Google Colab** (supuesto S-01). También corre en Linux o
  macOS. En Windows, PySpark necesita `winutils.exe` para escribir Parquet: usar Colab.

## Quickstart

### La forma más simple: Colab

Abrir `notebooks/00_pipeline_colab.ipynb` en Colab y ejecutar todo. Instala Java,
clona el repo, corre el pipeline dos veces (idempotencia), los tests y la carga a
AstraDB. El token se lee de Colab Secrets.

### Local, con un solo comando

```bash
pip install -r requirements.txt
python -m src.pipeline --reset                  # pasos 1 a 5 desde cero (~6 min)
python -m src.pipeline --reset --with-serving   # + AstraDB (requiere credenciales, paso 6)
```

### Paso por paso

Todos los comandos se corren desde la raíz del repo.

### 1. Ingesta batch → Bronze

```bash
python -m src.ingest.batch_to_bronze
```

Lee las 7 fuentes CSV de `data/landing/` con esquema explícito y escribe:

- `data/bronze/<fuente>/ingest_date=YYYY-MM-DD/` — Parquet, un archivo por corrida
- `data/quarantine/<fuente>/ingest_date=.../regla=.../` — filas no parseables,
  con clave nula o con clave duplicada, cada una con su motivo
- `evidence/runs/batch_to_bronze_<fecha>_<hora>.json` — conteos, rutas y tamaños

Es idempotente: re-ejecutar el mismo día reemplaza solo esa partición. Para fijar
la fecha: `INGEST_DATE=2026-11-12 python -m src.ingest.batch_to_bronze`.

### 2. Streaming de eventos → Bronze

```bash
python -m src.ingest.stream_to_bronze
```

Lee `data/landing/usage_events_stream/` con Structured Streaming, un archivo por
micro-lote, procesa lo pendiente y termina (`availableNow`). Escribe:

- `data/bronze/usage_events/ingest_date=YYYY-MM-DD/` — eventos válidos, deduplicados por `event_id`
- `data/quarantine/usage_events/ingest_date=.../regla=.../` — JSON roto, `event_id` nulo o timestamp inválido
- `_checkpoints/stream_to_bronze/` — qué archivos ya se procesaron y el estado de deduplicación

Re-ejecutar no reprocesa nada. Para simular llegada en vivo:
`python -m src.ingest.stream_to_bronze --continuous 30` y copiar archivos nuevos a la carpeta.

### 3. Bronze → Silver

```bash
python -m src.transform.silver_job
```

Recomputa Silver completo desde Bronze (idempotente):

- `data/silver/usage_events/event_date=.../service=.../` — eventos tipados, v1/v2
  compatibles, enriquecidos con la organización, con las features `requests`,
  `cpu_hours`, `storage_gb_hours`, `cost_usd`, `genai_tokens`, `carbon_kg` y la
  columna `quality_flags`
- `data/silver/customers_orgs/` — NPS resuelto (última encuesta, si no CRM; D-006)
- `data/silver/resources/` — `tags_json` convertido a array
- `data/quarantine/silver_usage_events/regla=.../` — copias de auditoría de los
  eventos marcados (`value_nulo`, `unit_nulo_con_value`, ...)
- `data/silver/billing_monthly/month=.../` — facturas normalizadas a USD (D-007, D-019)
- `data/silver/support_tickets/`, `users/` (sin email) y `marketing_touches/` — con
  las invariantes entre columnas de D-022 (CSAT 1-5, `last_login >= created_at`,
  `converted ⟹ clicked`)
- `data/quarantine/silver_<entidad>/regla=.../` — copias de auditoría de esos maestros

### 4. Silver → Gold `org_daily_usage_by_service`

```bash
python -m src.transform.gold_stream   # flujo C: capa rápida con watermark de 7 días
python -m src.transform.gold_job      # flujo D: reconciliación, mart oficial
```

- `data/gold/org_daily_usage_by_service/usage_date=.../` — mart reconciliado, grano
  (org_id, usage_date, service), con `last_reconciled_at`
- `data/gold/org_daily_usage_by_service_speed/batch_id=.../` — salida del streaming
  (valor provisorio; los eventos tardíos quedan fuera y se cuentan en el log)

### 4c. Silver → Gold: revenue, tickets y GenAI

```bash
python -m src.transform.gold_domains_job
```

Tres marts con control de grano y de conservación contra Silver (D-023):

- `data/gold/revenue_by_org_month/month=.../` — revenue en USD por (org, mes), con
  créditos, impuestos y FX aplicado (P4)
- `data/gold/tickets_by_org_date/` — tickets, SLA breach rate y CSAT promedio por
  (org, día de apertura, severidad) (P3)
- `data/gold/genai_tokens_by_org_date/usage_date=.../` — tokens y costo estimado por
  (org, día), desde 2025-07-18 (P5)

### 5. Anomalías de costo → `cost_anomaly_mart`

```bash
python -m src.analytics.anomalies
```

MAD sobre la serie diaria de cada (organización, servicio), |score| > 3,5 (D-010,
D-016). Escribe `data/gold/cost_anomaly_mart/date=.../` con score, línea de base y
flag por celda, y en el log la comparación contra z-score y MAD globales.

### 6. Serving en AstraDB

Requiere una base en AstraDB con el keyspace `cloud_analytics`, el token y el
Secure Connect Bundle. Credenciales solo por variables de entorno:

```bash
export ASTRA_DB_BUNDLE_PATH=/ruta/secure-connect-xxx.zip
export ASTRA_DB_TOKEN=AstraCS:...             # rol Database Administrator (carga)
export ASTRA_DB_TOKEN_READONLY=AstraCS:...    # rol Read Only User (consultas, D-025)
python -m src.serving.load_cassandra --with-speed   # crea las 5 tablas y las carga
python -m src.serving.run_queries                   # Q1 a Q5 -> evidence/serving/
```

| Consulta (§7.4) | Tabla | Clave |
|---|---|---|
| Q1 costos y requests diarios | `org_daily_usage_by_service` | `((org_id), usage_date, service)` |
| Q2 Top-N servicios en 14 días | `top_services_by_org_14d` | `((org_id, as_of_date), rank)` |
| Q3 tickets críticos y SLA, 30 días | `tickets_by_severity_date` | `((severity), ticket_date)` |
| Q4 revenue mensual en USD | `revenue_by_org_month` | `((org_id), month)` |
| Q5 tokens GenAI y costo por día | `genai_tokens_by_org_date` | `((org_id), usage_date)` |

Q3, Q4 y Q5 tienen además una partición de total (`'all'`), precalculada en Spark
(D-018, D-024).

Tablas y consultas en `infra/cql/`. Sin AstraDB: `CASSANDRA_HOST=localhost` contra
una Cassandra local.

### Todo junto en Colab

`notebooks/00_pipeline_colab.ipynb` instala Java 17, clona el repo y corre los
pasos 1 a 6 desde cero, incluida la re-ejecución para verificar idempotencia. El
token se lee de Colab Secrets.

### Pruebas

```bash
python -m pytest tests -q
```

### Reinicio

Las zonas derivadas se regeneran desde Landing:

```bash
rm -rf data/bronze data/silver data/gold data/quarantine _checkpoints
# Borrar los checkpoints sin borrar las zonas duplicaría eventos al re-ejecutar.
```

Configuración: ver `config/.env.example`.

### Perfilado (TP1)

Perfilado exploratorio de las fuentes de Landing:

```bash
python notebooks/01_perfilado_fuentes.py
```

Genera `evidence/perfilado_landing.md` y `evidence/perfilado_metrics.json`.
Lee Landing en modo read-only y es idempotente.

Rutas configurables por variable de entorno: `LANDING_PATH`, `EVIDENCE_PATH`.

## Solución de problemas

| Síntoma | Causa | Qué hacer |
|---|---|---|
| `UnsupportedClassVersionError` o Spark no arranca | Java 11 o anterior | Instalar Java 17 (Colab: la primera celda del notebook lo hace) |
| Error al escribir Parquet en Windows | Falta `winutils.exe` | Usar Colab (entorno de referencia, S-01) |
| `PATH_NOT_FOUND .../bronze/billing_monthly` en `silver_job` | Bronze incompleto: Silver necesita las 7 fuentes | Correr antes `python -m src.ingest.batch_to_bronze` (sin filtrar fuentes) |
| `ConservationError` y el job termina con código 1 | Un control de grano o de cuadre no se cumplió | Ver el detalle en el log y en `evidence/runs/<job>_*.json`; no se publica nada a medias |
| La segunda corrida del streaming procesa 0 micro-lotes | Es lo esperado: el checkpoint recuerda los archivos ya leídos | Para reprocesar desde cero: `python -m src.pipeline --reset`. **No** borrar solo los checkpoints |
| La capa rápida difiere ±0,1% entre dos clones | El orden de lectura depende de la fecha de los archivos (D-004) | Esperable; la cifra oficial es la del mart reconciliado, que no varía |
| `Faltan credenciales. Definí ASTRA_DB_BUNDLE_PATH...` | Variables de AstraDB sin definir | Ver `config/.env.example`; en Colab, Secrets con acceso habilitado al notebook |
| Los Secrets de Colab no aparecen | Se guardan por cuenta de Google | Crearlos de nuevo con la cuenta que corre el notebook |
| No se encuentra el Secure Connect Bundle | La consola de Astra lo muestra por base y región | En la base: *Overview → Regions → ⋯ → Download SCB* |
| Timeout o `NoHostAvailable` al conectar | La base de AstraDB está hibernada | Reanudarla desde la consola y esperar a que figure `ACTIVE` |
| La evidencia dice "Token usado: administrador" | Falta `ASTRA_DB_TOKEN_READONLY` | Crear un token con rol *Read Only User* (D-025) |
| `FutureWarning` de pandas al iniciar Spark | Aviso de PySpark sobre pandas 3 | Inofensivo: el pipeline no usa pandas |

## Limitaciones

Declaradas en `DECISIONS.md`; ninguna impide responder las cinco consultas.

- **Tasa ARS sospechosa (D-019).** Los montos en ARS parecen estar en otra escala: una
  factura queda en ~USD 1,19. Son el 21% de las facturas y el 0,05% del revenue. Se
  aplica la tasa de la fuente, sin corregirla.
- **Notas de crédito con impuestos positivos (D-019).** 13 facturas; se marcan y no se corrigen.
- **CSAT fuera de escala (D-022).** 40 tickets con CSAT 0, 6 o 7: el ticket cuenta, su CSAT no.
- **GenAI desde el 2025-07-18 (D-020).** Los eventos v1 no traen tokens; su costo está
  en `org_daily_usage_by_service`, no en el mart de tokens.
- **Capa rápida incompleta por diseño (D-015).** Los archivos llegan sin orden temporal y
  el watermark de 7 días descarta el 85% como tardío; la reconciliación batch los recupera.
- **Anomalías sin etiquetas (D-016).** Se detectan los 54 picos inyectados, pero sin
  etiquetas no hay precisión medible; la línea de base usa la serie completa.
- **"Últimos N días" anclados al fin del dataset** (2025-08-31) en Q2 y Q3, no a la fecha actual.
- **Escala no ejercitada.** 13 MB de datos en Spark `local[*]`: el diseño apunta a escala,
  pero no se midió en un clúster. Sin orquestador (cadencia manual con `src.pipeline`).
- **Entorno sin permisos por usuario.** En Colab el lake vive en el disco de la sesión;
  la matriz de acceso de `docs/gobierno.md` se cumple por construcción, no por IAM. La
  retención por zona está declarada pero no se puede ejercitar con 4 meses de datos.
- **Sin SCD (D-022).** Los maestros llegan como una sola foto; SCD tipo 2 quedó como deseable.

## Convenciones

- **Landing es inmutable.** Ningún proceso escribe en `data/landing/`.
- Esquemas **explícitos** en toda lectura; nunca `inferSchema` fuera de exploración.
- Nombres de archivos, tablas y columnas en `snake_case`.
- Zonas del Data Lake: `landing/` → `bronze/` → `silver/` → `gold/`.
- Toda decisión técnica con alternativa descartada se registra en `DECISIONS.md`.
- Sin credenciales, tokens ni secretos en el repositorio.

## Primera entrega

| Archivo | Contenido |
|---|---|
| `docs/Entregable1_BigData_Grupo4.pdf` | Documento de diseño entregado |
| `docs/diagrama_arquitectura_v1.svg` / `.png` / `.md` | Diagrama de arquitectura v1 |
| `notebooks/01_perfilado_fuentes.py` | Perfilado de las fuentes |
| `evidence/perfilado_landing.md` | Evidencia de exploración de datos |

## Segunda entrega

| Archivo | Contenido |
|---|---|
| `docs/Entregable2_BigData_Grupo4.pdf` / `.docx` | **Documento de la entrega** (9 páginas) |
| `docs/Entregable2_Anexos_BigData_Grupo4.pdf` / `.docx` | Anexos A–E: evidencias, quarantine, serving, diccionario y linaje, backlog |
| `src/`, `tests/`, `infra/cql/` | Pipeline, 47 pruebas y CQL de serving |
| `notebooks/00_pipeline_colab.ipynb` | Ejecución completa en Colab |
| `DECISIONS.md` | D-001 a D-017 (D-011 en adelante, de esta entrega) y decisiones abiertas |
| `docs/plan_correcciones.md` | Correcciones derivadas del feedback de la 1.ª entrega |
| `docs/backlog_final.md` | Backlog priorizado para la entrega final (obligatorio, deseable, fuera de alcance) |
| `docs/diagrama_arquitectura_v2.png` / `.svg` / `.md` | Arquitectura implementada y cambios respecto de la v1 |
| `docs/gobierno.md` | Responsables, metadatos, linaje, controles y acceso |
| `docs/diccionario_datos.md` | 25 datasets, 292 columnas (generado desde `src/governance/catalog.py`) |
| `evidence/runs/` | Log JSON de cada corrida: conteos, rutas, tamaños, tiempos |
| `evidence/idempotencia.md` | Pipeline y AstraDB ejecutados dos veces: variación 0 |
| `evidence/quarantine_muestras.md` | Muestras por regla de calidad |
| `evidence/linaje_trazado.md` | Una anomalía seguida de Gold hasta la línea de Landing |
| `evidence/a07_orden_eventos.md` | Cierre de A-07: orden temporal de los 120 archivos |
| `evidence/serving/` | Consultas CQL, resultados, latencias y capturas de AstraDB |

## Entrega final

| Archivo | Contenido |
|---|---|
| `docs/EntregaFinal_BigData_Grupo4.pdf` / `.docx` | **Documentación técnica final** (arquitectura, pipeline, marts, serving, analítica, calidad, gobierno, decisiones y limitaciones) |
| `docs/EntregaFinal_Anexos_BigData_Grupo4.pdf` / `.docx` | Anexos A–E: evidencias, quarantine, serving con las cinco consultas, diccionario y linaje, cierre del backlog |
| `docs/EntregaFinal_Presentacion_Grupo4.pptx` | Presentación ejecutiva (18 diapositivas en 5 bloques, con notas del orador) |
| `docs/diagrama_arquitectura_v3.png` / `.svg` / `.md` | Arquitectura final, sin componentes pendientes, y cambios respecto de la v2 |
| `src/`, `tests/`, `infra/cql/` | Pipeline completo, 64 pruebas, CQL de las cinco tablas y las cinco consultas |
| `notebooks/00_pipeline_colab.ipynb` | Ejecución completa en Colab, incluido el serving con token de solo lectura |
| `DECISIONS.md` | D-001 a D-025 (D-018 en adelante, de esta entrega); sin decisiones abiertas |
| `docs/plan_correcciones.md` | Correcciones del feedback de las dos entregas parciales |
| `docs/backlog_final.md` | Backlog con el estado de cierre de cada ítem |
| `docs/gobierno.md` | Gobierno, versión final: responsables, metadatos, linaje, controles, seguridad, observabilidad |
| `docs/diccionario_datos.md` | 39 datasets, 447 columnas (generado desde `src/governance/catalog.py`) |
| `evidence/idempotencia.md` | Pipeline y AstraDB ejecutados dos veces: variación 0 en 23 tablas |
| `evidence/serving/` | Q1 a Q5: CQL, resultados, latencias, token usado y capturas de AstraDB |
