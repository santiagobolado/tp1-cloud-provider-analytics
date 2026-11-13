# Muestras de quarantine

Generado: 2026-11-13T18:57:36+00:00 · `python notebooks/03_muestras_quarantine.py`

Quarantine guarda, por regla, el registro y el motivo. En Bronze, lo que va a
quarantine **no entra** a la zona. En Silver es una **copia de auditoría**: el evento
sigue en Silver con su flag (D-009, D-014).

## 1. Silver · dataset real

| Regla | Registros | Tratamiento |
|---|---|---|
| `unit_nulo_con_value` | 2.038 | unidad imputada desde la métrica (D-014) |
| `value_nulo` | 877 | métrica en nulo, el costo promueve (D-009) |
| `costo_negativo` | 211 | flag de negocio, sin copia en quarantine (§8.7) |

### `unit_nulo_con_value` · 3 de 2.038 (cómo quedaron en Silver)

| event_id | org_id | service | metric | value_raw | unit | cost_usd | quality_flags |
|---|---|---|---|---|---|---|---|
| evt_003mc7ncrk0z | org_w3zp08j3 | genai | requests | 120.0 | count | 11.5385 | unit_nulo_con_value |
| evt_01xb4ljijao2 | org_axeqbnhl | compute | cpu_hours | 1.7567 | hours | 0.1357 | unit_nulo_con_value |
| evt_02fpkgce46vv | org_kdgigatj | analytics | cpu_hours | 1.1257 | hours | 0.0927 | unit_nulo_con_value |

### `value_nulo` · 3 de 877 (cómo quedaron en Silver)

| event_id | org_id | service | metric | value_raw | unit | cost_usd | quality_flags |
|---|---|---|---|---|---|---|---|
| evt_00kblszrjmnm | org_w3zp08j3 | storage | requests | None | count | 2.5033 | value_nulo |
| evt_011a3k5yas7h | org_ir4cowgz | database | requests | None | count | 5.5933 | value_nulo |
| evt_01gzvk108sad | org_874frhoc | database | storage_gb_hours | None | gb_hours | 0.9869 | value_nulo |

### `costo_negativo` · los 3 más negativos (flag, no quarantine)

| event_id | org_id | service | metric | value_raw | unit | cost_usd | quality_flags |
|---|---|---|---|---|---|---|---|
| evt_t5brta67j5l3 | org_0lvsnujz | analytics | requests | 121.0 | count | -154.4608 | costo_negativo |
| evt_9ur456xoexl5 | org_h2m5zja8 | genai | requests | 140.0 | count | -19.8245 | costo_negativo |
| evt_zdg3w43spvdt | org_rixa11dp | genai | requests | 122.0 | count | -16.147 | costo_negativo |

## 2. Bronze · demostración con filas sucias conocidas

El dataset real no tiene rechazos en Bronze (0 en las 8 fuentes). Para mostrar que
las reglas funcionan, se agregan estas filas a una **copia temporal** de Landing:

### `customers_orgs.csv` · 3 filas agregadas, 3 en quarantine, 80 en Bronze

| entity | regla | detalle | raw_record | record_json |
|---|---|---|---|---|
| customers_orgs | clave_duplicada | clave natural repetida: ['org_id'] | None | {"org_id":"org_xaji0y6d","org_name":"Duplicada","industry":"Media","hq_region":"us-east","… |
| customers_orgs | clave_nula | clave natural incompleta: ['org_id'] | None | {"org_name":"Sin Clave","industry":"Media","hq_region":"us-east","plan_tier":"pro","is_ent… |
| customers_orgs | no_parseable | la fila no respeta el esquema declarado | org_demo_1,Fecha Rota,Media,us-east,pro,True,2025-13-45,rep_a,active,organic,10.0 | {"org_id":"org_demo_1","org_name":"Fecha Rota","industry":"Media","hq_region":"us-east","p… |

### `usage_events_stream` · 4 líneas: 1 válida a Bronze, 3 en quarantine

| entity | regla | raw_record | record_json |
|---|---|---|---|
| usage_events | event_id_nulo | None | {"timestamp":"2025-08-01T10:00:00Z","org_id":"org_xaji0y6d","resource_id":"res_x","service… |
| usage_events | no_parseable | {"event_id": "evt_demo_roto", "timestamp":  | {} |
| usage_events | timestamp_invalido | None | {"event_id":"evt_demo_ts","timestamp":"ayer","org_id":"org_xaji0y6d","resource_id":"res_x"… |

