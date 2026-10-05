# Diagrama de arquitectura — v1

**Versión**: 1.0 · **Fecha**: 2026-09-20 · **Estado**: propuesta para la 1.ª entrega

Este diagrama representa la arquitectura **propuesta**, no una implementación
existente. Se versiona junto con el código y debe actualizarse en la 2.ª entrega
para reflejar lo efectivamente construido, según exige el criterio de aceptación
"el diagrama representa lo implementado y no componentes meramente aspiracionales".

---

## Vista general

```mermaid
flowchart LR
  subgraph FUENTES["FUENTES"]
    direction TB
    M["Maestros y hechos batch<br/>customers_orgs · users · resources<br/>support_tickets · marketing_touches<br/>nps_surveys · billing_monthly<br/><i>CSV</i>"]
    E["Eventos de uso<br/>usage_events_stream/*.jsonl<br/><i>~20 archivos · v1 y v2</i>"]
  end

  subgraph INGESTA["INGESTA"]
    direction TB
    IB["Batch<br/>spark.read<br/>esquema explícito"]
    IS["Streaming<br/>spark.readStream<br/>watermark · dedupe · checkpoint"]
  end

  subgraph LAKE["DATA LAKE · Parquet particionado"]
    direction TB
    L["<b>LANDING</b><br/>crudo inmutable"]
    B["<b>BRONZE</b><br/>tipado · dedupe<br/>ingest_ts · source_file"]
    S["<b>SILVER</b><br/>conformado · joins<br/>compatibilidad v1/v2<br/>features"]
    G["<b>GOLD</b><br/>marts de negocio"]
    Q[("QUARANTINE<br/>inválidos con motivo")]
  end

  subgraph PROC["PROCESAMIENTO · PySpark"]
    direction TB
    P1["Normalización<br/>y conformación"]
    P2["Cálculo de features"]
    P3["Detección de<br/>anomalías"]
  end

  subgraph SERVING["SERVING"]
    C[("Cassandra / AstraDB<br/>modelado query-first")]
  end

  subgraph CONSUMO["CONSUMO"]
    direction TB
    U1["FinOps<br/>P1 P2 P4 P6"]
    U2["Soporte<br/>P3 P7"]
    U3["Producto / Usage<br/>P5"]
  end

  M --> IB --> L
  E --> IS --> L
  L --> B --> S --> G --> C
  B -.inválidos.-> Q
  S -.inválidos.-> Q
  S --- P1 & P2 & P3
  C --> U1 & U2 & U3

  classDef zona fill:#eef4ff,stroke:#4a6fa5,stroke-width:1px
  classDef store fill:#fff6e6,stroke:#b8860b,stroke-width:1px
  class L,B,S,G zona
  class C,Q store
```

## Capacidades transversales

Atraviesan todas las etapas de la cadena y no son una etapa más.

```mermaid
flowchart TB
  subgraph T["CAPACIDADES TRANSVERSALES"]
    direction LR
    T1["<b>Calidad</b><br/>reglas verificables<br/>invariantes entre columnas<br/>quarantine con motivo"]
    T2["<b>Gobierno</b><br/>responsables por dominio<br/>reglas de promoción<br/>retención por zona"]
    T3["<b>Metadatos y linaje</b><br/>ingest_ts · source_file<br/>diccionario de datos<br/>trazabilidad Landing→Gold"]
    T4["<b>Seguridad</b><br/>config externalizada<br/>sin secretos en el repo<br/>acceso por rol"]
    T5["<b>Observabilidad</b><br/>conteos por etapa<br/>logs · métricas de corrida<br/>evidencia de ejecución"]
  end
  T -.->|"aplican sobre"| X["Ingesta → Data Lake → Procesamiento → Serving → Consumo"]
```

## Rutas y latencias

```mermaid
flowchart LR
  A["Eventos de uso"] -->|"micro-lotes<br/><b>≤ 5 min</b> (O1)"| B["Gold FinOps / Producto"]
  C["Maestros y hechos"] -->|"lote diario"| D["Gold Soporte · dimensiones"]
  E["Facturación"] -->|"lote mensual"| F["Gold revenue"]
  B & D & F --> G[("Cassandra")]
```

## Leyenda de trazabilidad

| Elemento | Responde a |
|---|---|
| Ruta streaming | V-Velocidad · O1 · P1, P2, P5, P6 |
| Ruta batch diaria | V-Variedad · O2 · P3, P7 |
| Ruta batch mensual | V-Veracidad (FX, créditos) · P4 |
| Quarantine | V-Veracidad · O3 |
| Checkpoint + dedupe | O4 |
| Modelado query-first | V-Valor · O5, O6 |
| Silver (compatibilidad v1/v2) | V-Variedad · O8 |

El detalle de cada correspondencia está en la matriz requisito–componente de la §7.
