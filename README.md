# Cloud Provider Analytics

Proyecto integrador de **Big Data** · ITBA · 2.º cuatrimestre 2026
Pipeline de ETL, streaming y serving para analítica de FinOps, Soporte y Producto
sobre los datos de un proveedor de nube.

## Estado

| Entrega | Fecha | Estado |
|---|---|---|
| 1ª parcial — Diseño y fundación | 28/09/2026 18:30 | Entregada |
| 2ª parcial — Implementación técnica | 16/11/2026 18:30 | Pipeline end-to-end ejecutable y verificado en AstraDB |
| Final — MVP integrado y defensa | 07/12/2026 21:30 | — |

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

### 4. Silver → Gold `org_daily_usage_by_service`

```bash
python -m src.transform.gold_stream   # flujo C: capa rápida con watermark de 7 días
python -m src.transform.gold_job      # flujo D: reconciliación, mart oficial
```

- `data/gold/org_daily_usage_by_service/usage_date=.../` — mart reconciliado, grano
  (org_id, usage_date, service), con `last_reconciled_at`
- `data/gold/org_daily_usage_by_service_speed/batch_id=.../` — salida del streaming
  (valor provisorio; los eventos tardíos quedan fuera y se cuentan en el log)

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
export ASTRA_DB_TOKEN=AstraCS:...
python -m src.serving.load_cassandra --with-speed   # crea tablas y carga P1 y P2
python -m src.serving.run_queries                   # Q1 y Q2 -> evidence/serving/
```

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
| `src/`, `tests/`, `infra/cql/` | Pipeline, 42 pruebas y CQL de serving |
| `notebooks/00_pipeline_colab.ipynb` | Ejecución completa en Colab |
| `DECISIONS.md` | D-001 a D-017 (D-011 en adelante, de esta entrega) y decisiones abiertas |
| `docs/plan_correcciones.md` | Correcciones derivadas del feedback de la 1.ª entrega |
| `docs/checklist_entrega_2.md` | Verificación contra el anexo 9.2 de la consigna |
| `evidence/runs/` | Log JSON de cada corrida: conteos, rutas, tamaños, tiempos |
| `evidence/idempotencia.md` | Pipeline y AstraDB ejecutados dos veces: variación 0 |
| `evidence/quarantine_muestras.md` | Muestras por regla de calidad |
| `evidence/a07_orden_eventos.md` | Cierre de A-07: orden temporal de los 120 archivos |
| `evidence/serving/` | Consultas CQL, resultados, latencias y capturas de AstraDB |
