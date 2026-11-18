# Plan de correcciones — feedback de la 1.ª entrega

Consigna §5.6: prioridad, responsable, fecha objetivo y evidencia esperada.

| # | Corrección | Origen | Prioridad | Responsable | Fecha | Evidencia esperada | Estado |
|---|---|---|---|---|---|---|---|
| C-01 | Documento principal de 10-12 páginas; detalle secundario en anexos | Feedback del profesor ("Ingeniería y documentación", 8,5) | Alta | R1 | 16/11 | `docs/Entregable2_BigData_Grupo4.pdf` (9 págs.) + `docs/Entregable2_Anexos_BigData_Grupo4.pdf` (A–E) | hecho |
| C-02 | Cifras de duplicados 17/61/9 heredadas de `approx_count_distinct`; el conteo exacto da 0 | Revisión interna | Media | R2 | 11/11 | D-011 en `DECISIONS.md`; conteos en `evidence/runs/` | hecho |
| C-03 | D-005 dejaba los maestros sin particionar; la consigna exige Parquet particionado | Revisión contra consigna §6.2 | Alta | R2 | 11/11 | D-011; Bronze en `ingest_date=` | hecho |
| C-04 | Cerrar A-07 (orden cronológico de los archivos de eventos) | Decisión abierta del TP1 | Alta | R2 | 13/11 | `evidence/a07_orden_eventos.md`; D-012 | hecho |
| C-05 | Diagrama v2 consistente con lo implementado | Consigna §6.2 | Alta | R1 | 15/11 | `docs/diagrama_arquitectura_v2.{svg,png,md}`, con la tabla de cambios respecto de la v1 | hecho |
| C-06 | Volumen de eventos: el TP1 estimó ~20 archivos y ~7.200 eventos; son 120 y 43.200 | Perfilado del dataset completo | Media | R1 | 16/11 | Tabla de cifras corregidas en la §2 del documento | hecho |

## Feedback de la 2.ª entrega

Nota 10/10 (rúbrica 9,98). Todas las dimensiones en 10 salvo Defensa (9,5): el
profesor reservó medio punto hasta ver cómo se reparte el conocimiento técnico
entre los cuatro integrantes. Recomendación para el final: no sumar
sofisticación; completar el alcance funcional restante y mantener la coherencia
entre catálogo, código, tests, Gold y serving.

| # | Corrección | Origen | Prioridad | Responsable | Fecha | Evidencia esperada | Estado |
|---|---|---|---|---|---|---|---|
| C-07 | Repartir el conocimiento: cada integrante es dueño de un rol (R1–R4) y suplente de otro, y los cuatro pueden explicar el recorrido completo | Feedback (Defensa, 9,5) | Alta | los 4 | 22/11 | Guion de defensa con roles; ensayo con preguntas cruzadas | pendiente |
| C-08 | Recortar el alcance a lo funcional: los deseables del backlog no se hacen antes del envío; GenAI solo batch (D-020) | Recomendación del profesor | Alta | R1 | 17/11 | `docs/backlog_final.md` recalendarizado; D-020 | hecho |
| C-09 | Coherencia entre catálogo, código, tests, Gold y serving para los tres marts nuevos | Recomendación del profesor | Alta | R1 / R2 | 21/11 | `test_governance` en verde con los 5 marts; diccionario regenerado | pendiente |
| C-10 | `evidence/idempotencia.md` cita el commit `78becbf`; el código actual está en otro commit | Revisión interna | Baja | R4 | 20/11 | Evidencia regenerada con la corrida final | pendiente |
