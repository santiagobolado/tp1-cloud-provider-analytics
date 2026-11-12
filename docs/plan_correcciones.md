# Plan de correcciones — feedback de la 1.ª entrega

Consigna §5.6: prioridad, responsable, fecha objetivo y evidencia esperada.

| # | Corrección | Origen | Prioridad | Responsable | Fecha | Evidencia esperada | Estado |
|---|---|---|---|---|---|---|---|
| C-01 | Documento principal de 10-12 páginas; detalle secundario en anexos | Feedback del profesor ("Ingeniería y documentación", 8,5) | Alta | R1 | 16/11 | `docs/entrega2.pdf` ≤ 12 págs. + anexos A-E | pendiente |
| C-02 | Cifras de duplicados 17/61/9 heredadas de `approx_count_distinct`; el conteo exacto da 0 | Revisión interna | Media | R2 | 11/11 | D-011 en `DECISIONS.md`; conteos en `evidence/runs/` | hecho |
| C-03 | D-005 dejaba los maestros sin particionar; la consigna exige Parquet particionado | Revisión contra consigna §6.2 | Alta | R2 | 11/11 | D-011; Bronze en `ingest_date=` | hecho |
| C-04 | Cerrar A-07 (orden cronológico de los archivos de eventos) | Decisión abierta del TP1 | Alta | R2 | 13/11 | `evidence/a07_orden_eventos.md`; D-012 | hecho |
| C-05 | Diagrama v2 consistente con lo implementado | Consigna §6.2 | Alta | R1 | 15/11 | `docs/diagrama_arquitectura_v2` | pendiente |
| C-06 | Volumen de eventos: el TP1 estimó ~20 archivos y ~7.200 eventos; son 120 y 43.200 | Perfilado del dataset completo | Media | R1 | 16/11 | §3.2 corregida en el documento de la 2.ª entrega | pendiente |
