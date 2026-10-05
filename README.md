# Cloud Provider Analytics

Proyecto integrador de **Big Data** · ITBA · 2.º cuatrimestre 2026
Pipeline de ETL, streaming y serving para analítica de FinOps, Soporte y Producto
sobre los datos de un proveedor de nube.

## Estado

| Entrega | Fecha | Estado |
|---|---|---|
| 1ª parcial — Diseño y fundación | 28/09/2026 18:30 | ✅ lista para entregar |
| 2ª parcial — Implementación técnica | 16/11/2026 18:30 | ⬜ |
| Final — MVP integrado y defensa | 07/12/2026 21:30 | ⬜ |

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

- Python 3.10+
- Java 17 o superior (requerido por Spark)
- PySpark 4.2.0

```bash
pip install pyspark
```

## Ejecución

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
| `docs/diseno_tp1.pdf` | **Documento de diseño — entregable** |
| `docs/diseno_tp1.md` | Fuente editable del documento |
| `docs/diagrama_arquitectura_v1.svg` / `.png` | Diagrama de arquitectura v1 |
| `docs/diagrama_arquitectura_v1.md` | Diagrama en Mermaid, versionado como texto |
| `docs/checklist_entrega_1.md` | Verificación contra el anexo 9.1 de la consigna |
| `docs/defensa_completa.md` / `.pdf` | Material interno de defensa (no se entrega) |
| `DECISIONS.md` | 10 decisiones cerradas y 3 abiertas |
| `evidence/perfilado_landing.md` | Evidencia de exploración de datos |
