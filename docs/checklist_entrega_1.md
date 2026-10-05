# Verificación contra el checklist de la consigna

**Anexo 9.1 · Primera evaluación · 28/09/2026 · 18:30 h**
Verificado el 2026-09-20 · Versión 1.0 del documento de diseño

---

## Los 12 ítems

| # | Ítem del checklist | Estado | Dónde se cumple |
|---|---|---|---|
| 1 | Documento de diseño disponible en el canal de entrega | ⚠️ | `docs/diseno_tp1.pdf` — listo; **falta subirlo** |
| 2 | Repositorio accesible y versionado | ⚠️ | Estructura completa; **falta publicarlo y compartir acceso** |
| 3 | Interpretación del caso y objetivos medibles | ✅ | §2 — 7 preguntas P1–P7, 8 objetivos con umbral |
| 4 | Análisis 5V | ✅ | §3 — cada V con evidencia y decisión que fuerza |
| 5 | Inventario y perfil de fuentes | ✅ | §4 — las 8 fuentes, 10 riesgos de calidad |
| 6 | Arquitectura v1 y patrón justificado | ✅ | §5 y §6 — híbrido, con Kappa y Lambda descartados |
| 7 | Diseño Landing/Bronze/Silver/Gold | ✅ | §8 — zonas, particionado, naming, retención, promoción |
| 8 | Flujos batch y streaming | ✅ | §9 — cuatro flujos con herramientas específicas |
| 9 | Lógica MapReduce o equivalente | ✅ | §10 — mapper, combiner, partitioner, reducer y trazado |
| 10 | Matriz requisito-componente | ✅ | §7 — cuatro matrices encadenadas |
| 11 | Supuestos, riesgos, mitigaciones y estimación de esfuerzo | ✅ | §11 y §12 — 7 supuestos, 8 riesgos, 286 h-persona |
| 12 | Evidencia mínima de lectura/exploración de datos | ✅ | §13 + `evidence/` + `notebooks/01_perfilado_fuentes.py` |

**10 de 12 cumplidos.** Los dos pendientes son de publicación, no de contenido.

---

## Los 5 artefactos exigidos (§5.3 de la consigna)

| Artefacto | Archivo | Estado |
|---|---|---|
| Documento de diseño | `docs/diseno_tp1.md` / `.pdf` | ✅ |
| Repositorio | este repositorio | ⚠️ falta publicar |
| Diagrama de arquitectura v1 | `docs/diagrama_arquitectura_v1.md` | ✅ |
| Matriz requisito–componente | §7 del documento | ✅ |
| Plan inicial | §11 y §12 del documento | ✅ |

---

## Criterios de aceptación (§5.4 de la consigna)

| Criterio | Evaluación |
|---|---|
| El problema, los usuarios y los criterios de éxito están formulados sin ambigüedad | ✅ §2: cada dominio con su decisión y latencia; objetivos con umbral e instancia de verificación |
| La arquitectura responde a los requisitos y distingue claramente batch y streaming | ✅ §5 y §6: cuatro flujos con cadencia declarada y patrón justificado contra alternativas |
| Las zonas del Data Lake, formatos y particiones son coherentes con los datos provistos | ✅ §8: el particionado se deriva del hallazgo de fragmentación aleatoria (D-005) |
| El flujo MapReduce muestra cómo se resolvería el procesamiento batch del caso | ✅ §10: `org_daily_usage_by_service` con trazado sobre registros reales |
| Los supuestos y riesgos son realistas y tienen mitigaciones propuestas | ✅ §11: 7 supuestos con consecuencia si son falsos, 8 riesgos con señal de alerta |
| El repositorio y la documentación permiten continuar sin rehacer la fundación | ✅ Estructura, README, `DECISIONS.md` con 10 decisiones, script de perfilado reproducible |

---

## Lo que falta antes del 28/09

| # | Tarea | Responsable | Bloquea |
|---|---|---|---|
| 1 | Publicar el repositorio y compartir acceso con la cátedra | R4 | Ítems 1 y 2 del checklist |
| 2 | Subir el documento al canal de entrega | R1 | Ítem 1 |
| 3 | Completar los nombres de los 4 integrantes en la portada | R1 | — |
| 4 | Conseguir los ~20 archivos de `usage_events_stream/` | R2 | A-07, no la entrega |
| 5 | Lectura cruzada del documento por los 4 integrantes | los 4 | — |

**Ninguno de los cinco es de contenido.** El punto 4 no bloquea esta entrega: A-07
está declarada como decisión abierta con su fecha, que es exactamente lo que la
consigna pide en el punto 10 del alcance.

---

## Autoevaluación contra la rúbrica (§5.5 de la consigna)

Estimación propia, para orientar dónde reforzar si queda tiempo.

| Dimensión | Peso | Fortaleza | Dónde podría flaquear |
|---|---|---|---|
| Comprensión y justificación | 15% | Las 5V con evidencia cuantificada; admitir que el volumen no justifica Big Data | Los umbrales de O1 y O6 son supuestos propios |
| Arquitectura | 25% | Patrón justificado con datos, no con preferencia; alternativas descartadas por escrito | El diagrama es propuesta, no implementación |
| Data Lake | 20% | El cambio de eje de partición sale de un hallazgo del perfilado | Retención no ejercitable con 4 meses |
| Datos | 15% | Perfilado ejecutado sobre las 8 fuentes, reproducible; 10 riesgos con magnitud | Un solo archivo de eventos (A-07) |
| Procesamiento batch | 10% | Trazado con registros reales; el contraste con Spark usa un caso del proyecto | — |
| Ingeniería y documentación | 10% | `DECISIONS.md` con 10 entradas y alternativas | Repositorio aún sin publicar |
| Defensa | 5% | Material de defensa consolidado con preguntas previstas | Requiere ensayo con los 4 integrantes |
