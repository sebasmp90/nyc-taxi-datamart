# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Data mart Finanzas TLC — 05 · Rendimiento, diseño físico y orquestación
# MAGIC
# MAGIC **Parte 5** (misma consulta sobre silver y sobre la estrella, planes, diseño físico) y **Parte 6** (DAG del
# MAGIC workflow).
# MAGIC
# MAGIC > Top 20 pares zona de origen → zona de destino con más viajes, excluyendo las zonas 264 y 265 en ambos extremos.
# MAGIC > Por cada par: viajes, distancia promedio, `total_amount` promedio y duración promedio.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F
import time, statistics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Las dos versiones
# MAGIC
# MAGIC - **Silver (tabla ancha)**: los nombres de zona ya vienen unidos; se excluye por `LocationID`.
# MAGIC - **Estrella**: `fact_trips` + `dim_zone` dos veces (role-playing); se excluye con `is_special_zone`, la regla
# MAGIC   de negocio que vive en la dimensión.
# MAGIC
# MAGIC El desempate `ORDER BY viajes DESC, pu, do` hace el top 20 determinístico (si hay empates en el puesto 20, ambas
# MAGIC versiones eligen el mismo par).

# COMMAND ----------

Q_SILVER = f"""
SELECT PULocationID AS pu_id, DOLocationID AS do_id,
       pickup_zone  AS zona_origen, dropoff_zone AS zona_destino,
       COUNT(*)                          AS viajes,
       ROUND(AVG(trip_distance), 4)      AS distancia_prom,
       ROUND(AVG(total_amount), 4)       AS total_amount_prom,
       ROUND(AVG(trip_duration_min), 4)  AS duracion_prom_min
FROM {T_SILVER_TRIPS_ENRICHED}
WHERE PULocationID NOT IN (264, 265) AND DOLocationID NOT IN (264, 265)
GROUP BY PULocationID, DOLocationID, pickup_zone, dropoff_zone
ORDER BY viajes DESC, pu_id, do_id
LIMIT 20
"""

Q_STAR = f"""
SELECT f.pickup_zone_key AS pu_id, f.dropoff_zone_key AS do_id,
       pz.zone AS zona_origen, dz.zone AS zona_destino,
       COUNT(*)                            AS viajes,
       ROUND(AVG(f.trip_distance), 4)      AS distancia_prom,
       ROUND(AVG(f.total_amount), 4)       AS total_amount_prom,
       ROUND(AVG(f.trip_duration_min), 4)  AS duracion_prom_min
FROM {T_DM_FACT_TRIPS} f
JOIN {T_DM_DIM_ZONE} pz ON f.pickup_zone_key  = pz.zone_key
JOIN {T_DM_DIM_ZONE} dz ON f.dropoff_zone_key = dz.zone_key
WHERE NOT pz.is_special_zone AND NOT dz.is_special_zone
GROUP BY f.pickup_zone_key, f.dropoff_zone_key, pz.zone, dz.zone
ORDER BY viajes DESC, pu_id, do_id
LIMIT 20
"""

r_silver = spark.sql(Q_SILVER)
r_star = spark.sql(Q_STAR)
display(r_star)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación: ambas versiones devuelven lo mismo

# COMMAND ----------

def normalize(df):
    return df.select(F.col("pu_id").cast("int"), F.col("do_id").cast("int"), "zona_origen", "zona_destino",
                     F.col("viajes").cast("long"),
                     *[F.round(F.col(c).cast("double"), 3).alias(c)
                       for c in ["distancia_prom", "total_amount_prom", "duracion_prom_min"]])

a, b = normalize(r_silver), normalize(r_star)
diff = a.exceptAll(b).count() + b.exceptAll(a).count()
assert a.count() == b.count() == 20 and diff == 0, f"Las versiones difieren en {diff} filas"
print("✓ Silver y estrella devuelven exactamente el mismo top 20")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Tiempo de ejecución y plan
# MAGIC
# MAGIC Cada versión se ejecuta 1 vez de calentamiento + 3 veces medidas (`collect()` fuerza la ejecución completa).
# MAGIC Se reporta la mediana. En serverless los tiempos varían entre corridas: lo importante es el orden de magnitud y
# MAGIC el plan, no la décima de segundo.
# MAGIC
# MAGIC **Dónde ver el plan gráfico:**
# MAGIC - En un **clúster clásico**: enlace "Spark Jobs" bajo la celda → *View* → **Spark UI** → pestaña
# MAGIC   **SQL / DataFrame** → la consulta.
# MAGIC - En **serverless** no hay Spark UI: bajo la celda, **See performance** → la consulta → **Query profile**.
# MAGIC
# MAGIC En ambos busca los nodos de broadcast. Con Photon (serverless y la mayoría de clústeres) se llaman
# MAGIC `PhotonBroadcastHashJoin` / `PhotonBroadcastExchange`; sin Photon, `BroadcastHashJoin` / `BroadcastExchange`.
# MAGIC Mira también, en el `Scan`, los archivos leídos y descartados (*files pruned*).

# COMMAND ----------

def time_query(sql, runs=3):
    spark.sql(sql).collect()                     # calentamiento (metadatos, caché de disco)
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        spark.sql(sql).collect()
        times.append(time.perf_counter() - t0)
    return statistics.median(times), times

t_silver, all_s = time_query(Q_SILVER)
t_star, all_t = time_query(Q_STAR)
display(spark.createDataFrame(
    [("Silver (tabla ancha)", round(t_silver, 3), str([round(x, 3) for x in all_s])),
     ("Estrella (fact + dim_zone x2)", round(t_star, 3), str([round(x, 3) for x in all_t]))],
    "version STRING, mediana_seg DOUBLE, corridas STRING"))
print(f"Estrella / silver = {t_star / t_silver:.2f}x")

# COMMAND ----------

import re

def plan(sql):
    return spark.sql(f"EXPLAIN FORMATTED {sql}").first()[0]

def count_nodes(p, node):
    # En EXPLAIN FORMATTED cada nodo tiene una sección de detalle "(n) Nodo"; se cuentan esas.
    # Con Photon el nodo lleva prefijo: PhotonBroadcastHashJoin, PhotonBroadcastExchange, ...
    return len(re.findall(rf"^\(\d+\) (?:Photon)?{node}\b", p, flags=re.M))

p_silver, p_star = plan(Q_SILVER), plan(Q_STAR)
for name, p in [("SILVER", p_silver), ("ESTRELLA", p_star)]:
    print(f"{name}: " + ", ".join(f"{n} = {count_nodes(p, n)}"
                                  for n in ["BroadcastHashJoin", "ShuffledHashJoin", "SortMergeJoin", "BroadcastExchange"]))

print("\n" + "=" * 30 + " PLAN ESTRELLA " + "=" * 30)
print(p_star)

# COMMAND ----------

print("=" * 30 + " PLAN SILVER " + "=" * 30)
print(p_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Dónde aparecen los broadcast joins?
# MAGIC
# MAGIC - **Silver**: no hay joins en la consulta (los nombres de zona ya vienen en la tabla ancha). El plan es
# MAGIC   `Scan → Filter → HashAggregate (parcial) → Exchange → HashAggregate (final) → TakeOrderedAndProject`.
# MAGIC - **Estrella**: aparecen **dos `BroadcastHashJoin`** (con Photon, `PhotonBroadcastHashJoin`), uno por cada
# MAGIC   rol de `dim_zone` (pickup y dropoff). La
# MAGIC   dimensión (~266 filas) pasa por un `BroadcastExchange` y se copia a cada ejecutor; la tabla de hechos **no se
# MAGIC   redistribuye** (no hay `SortMergeJoin` ni shuffle de los viajes por el join). El filtro `NOT is_special_zone` se
# MAGIC   aplica sobre la dimensión antes de difundirla y, gracias al broadcast, el optimizador puede convertirlo en un
# MAGIC   filtro dinámico sobre `pickup_zone_key` / `dropoff_zone_key` (*dynamic file pruning*). El único shuffle de
# MAGIC   ambos planes es el de la agregación.
# MAGIC
# MAGIC Completa con tus cifras: silver `____ s`, estrella `____ s`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Diseño físico de `fact_trips`
# MAGIC
# MAGIC **Decisión: liquid clustering con `CLUSTER BY (pickup_date_key, pickup_zone_key)`**, sin particionado ni `ZORDER`
# MAGIC (liquid clustering no se combina con ninguno de los dos). Se aplica al crear la tabla en el notebook 02 y se
# MAGIC mantiene con `OPTIMIZE`.
# MAGIC
# MAGIC | Pregunta (Parte 4) | Filtros / agrupaciones sobre la tabla de hechos |
# MAGIC |---|---|
# MAGIC | 1. Revenue mensual por borough | **fecha** (mes) + **zona de pickup** |
# MAGIC | 2. Ticket a aeropuertos en festivos | **fecha** (`is_holiday`) + zona de dropoff |
# MAGIC | 3. Tarifa JFK | `rate_code_key` + zonas |
# MAGIC | 4. Propinas por franja | `payment_type_key` + hora + **fecha** (fin de semana) + **zona de pickup** |
# MAGIC | 5. Mezcla de pagos por mes | **fecha** (mes) + `payment_type_key` |
# MAGIC | 6. Recargos por borough | **zona de pickup** (+ **fecha** para 2025) |
# MAGIC
# MAGIC Por qué estas columnas:
# MAGIC
# MAGIC - **`pickup_date_key`** aparece en casi todas las preguntas y es el filtro natural de Finanzas (cierres mensuales,
# MAGIC   "solo 2025", comparaciones año contra año). Tiene alta cardinalidad (~1 000 días) y los datos llegan por mes,
# MAGIC   así que el clustering agrupa bien los archivos y permite **saltarse archivos** por estadísticas min/max.
# MAGIC - **`pickup_zone_key`** es la segunda dimensión más usada (borough, aeropuertos) y tiene cardinalidad media (~265).
# MAGIC - **No** `payment_type_key`, `rate_code_key` ni `vendor_key`: muy pocos valores (2–8) y casi todo el volumen en
# MAGIC   uno o dos (tarjeta, tarifa estándar). El *data skipping* por min/max casi no descarta archivos.
# MAGIC - **Por qué no particionar por mes**: cada partición quedaría con pocos GB o menos (Databricks recomienda ≥ 1 GB por
# MAGIC   partición), se generan archivos pequeños, y la columna de partición queda fija: cambiarla obliga a reescribir
# MAGIC   la tabla. Con liquid clustering las llaves se cambian con `ALTER TABLE ... CLUSTER BY` y el `OPTIMIZE` es
# MAGIC   incremental.
# MAGIC - **Por qué no `ZORDER`**: no es incremental (cada `OPTIMIZE ZORDER BY` reescribe) y liquid clustering lo
# MAGIC   reemplaza en tablas nuevas.
# MAGIC
# MAGIC Detalle importante del modelo estrella: las preguntas filtran por **atributos de la dimensión** (`d.year = 2025`,
# MAGIC `pz.borough = 'Queens'`), no por la llave. El salto de archivos funciona igual porque, con la dimensión en
# MAGIC broadcast, Databricks traduce ese filtro en la lista de llaves que cumplen y lo empuja al scan de la tabla de
# MAGIC hechos (*dynamic file pruning*). Por eso conviene clusterizar por las **llaves** de las dimensiones más filtradas.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {T_DM_FACT_TRIPS}")
        .select("name", "numFiles", "sizeInBytes", "clusteringColumns", "partitionColumns"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta: ¿la estrella fue más rápida que la tabla ancha de silver? Si no, ¿por qué? ¿Qué se gana igual?
# MAGIC
# MAGIC Lo más probable es que **no**: queden parecidas o silver sea algo más rápida. Razones:
# MAGIC
# MAGIC - Delta/Parquet es **columnar**: la consulta sobre silver solo lee las 6–8 columnas que usa, así que la tabla
# MAGIC   "ancha" no se paga en lectura. Las columnas de nombres de zona se comprimen muy bien (diccionario).
# MAGIC - La estrella hace **dos joins extra**. Son baratos (broadcast de 266 filas), pero no gratis.
# MAGIC - Ambas consultas escanean todos los viajes y el costo dominante es el **scan + la agregación**, igual en las dos.
# MAGIC - La ventaja física de la estrella (claves `INT` en vez de textos, clustering) se nota más en consultas con filtros
# MAGIC   selectivos de fecha o zona, que aquí no hay.
# MAGIC
# MAGIC Lo que se gana con el mart aunque no gane en velocidad:
# MAGIC
# MAGIC - **Una sola fuente de reglas de negocio**: qué es aeropuerto, qué zonas son especiales, qué forma de pago registra
# MAGIC   propina, qué días son festivos. Cambian en una dimensión y aplican a todas las consultas.
# MAGIC - **Gobierno**: Finanzas consume vistas con `SELECT` y nunca toca silver; las constraints y comentarios documentan
# MAGIC   el modelo en el catálogo.
# MAGIC - **Preguntas nuevas sin tablas nuevas**: las seis preguntas de la Parte 4 salieron sin un notebook ni una tarea
# MAGIC   más en el workflow.
# MAGIC - **Consistencia**: todos los reportes cuadran entre sí porque salen de la misma tabla de hechos conciliada contra
# MAGIC   silver.
# MAGIC - **Dimensiones conformadas** reutilizables (calendario, zonas) por otros marts (por ejemplo, operaciones o
# MAGIC   green taxis), y un modelo que entienden las herramientas de BI.
# MAGIC
# MAGIC ### Pregunta: ¿cuándo seguirías creando una tabla agregada aunque tengas el mart?
# MAGIC
# MAGIC Cuando una **misma** pregunta se consulta muchas veces y la latencia o el costo importan más que la flexibilidad:
# MAGIC
# MAGIC - Un dashboard operativo de demanda por `zona × fecha × hora` que se refresca cientos de veces al día: recalcular
# MAGIC   desde ~100 M de viajes cada vez es caro; un agregado de ~265 × 1 000 × 24 filas responde en milisegundos.
# MAGIC - Servir a una aplicación o API con SLA de baja latencia.
# MAGIC - Métricas con lógica costosa (ventanas, percentiles, conteos distintos) que conviene calcular una vez.
# MAGIC
# MAGIC Pero el agregado se construye **desde el mart** (desde `fact_trips` y sus dimensiones), no desde silver: así hereda
# MAGIC las mismas reglas y siempre cuadra con él. En Databricks puede ser una **materialized view** que se refresca de forma
# MAGIC incremental. El mart es la fuente de verdad; el agregado es una caché de rendimiento.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 6 · Orquestación: DAG para `07-workflow/resources/nyctaxi_job.yml`
# MAGIC
# MAGIC ```
# MAGIC  bronze_trips ──► silver_trips_clean ──► silver_trips_enriched ──┬──────────────────────────────┐
# MAGIC                                                                   │                              │
# MAGIC  bronze_zones ────────────────────────────────────────────────────┤                              │
# MAGIC                                                                   ▼                              ▼
# MAGIC                       ┌─────────────── dm_dimensions (01) ───────────────┐      gold_revenue_by_zone
# MAGIC                       │  dim_date  dim_time  dim_zone  dim_payment_type  │      gold_daily_metrics
# MAGIC                       │  dim_rate_code  dim_vendor  (independientes)     │      gold_hourly_demand
# MAGIC                       └──────────────────────┬───────────────────────────┘      gold_ml_features
# MAGIC                                              ▼                                  (en paralelo con el mart)
# MAGIC                       dm_fact_trips (02) ── incluye la CONCILIACIÓN al final
# MAGIC                                              │   (si falla, se detiene aquí: no se publican vistas)
# MAGIC                                              ▼
# MAGIC                       dm_constraints_views (03) ── PK/FK RELY + vistas de consumo
# MAGIC                                              │
# MAGIC                                              ▼
# MAGIC                       (opcional) dm_business_checks (04) ── refresca/valida las consultas de Finanzas
# MAGIC ```
# MAGIC
# MAGIC - **Dependencias**: las dimensiones dependen de `silver_trips_enriched` (rango de fechas y verificaciones) y de
# MAGIC   `bronze_zones` (`dim_zone`). La tabla de hechos depende de **todas** las dimensiones y de silver. Constraints y
# MAGIC   vistas dependen de la tabla de hechos.
# MAGIC - **En paralelo**: la rama del mart corre en paralelo con las tablas gold agregadas y las features de ML (todas
# MAGIC   dependen solo de silver). Dentro de `dm_dimensions`, las seis dimensiones son independientes entre sí: si el
# MAGIC   volumen lo justificara, se separan en tareas paralelas con un parámetro.
# MAGIC - **Conciliación**: es la **última celda de `dm_fact_trips`** y hace fallar la tarea; por eso `dm_constraints_views`
# MAGIC   (que declara `RELY` y publica las vistas) solo corre si la conciliación pasó. Podría separarse en una tarea
# MAGIC   `dm_reconciliation` entre ambas, con el mismo efecto.
# MAGIC - **Fuera del job**: `GRANT` (Parte 3.3) es una configuración de una sola vez, no una tarea diaria. El notebook 05
# MAGIC   (benchmark) tampoco va en el job.
# MAGIC
# MAGIC Fragmento ilustrativo del YAML (ajusta los `task_key` existentes a los de tu `nyctaxi_job.yml`; no se modifica el
# MAGIC bundle):
# MAGIC
# MAGIC ```yaml
# MAGIC         - task_key: dm_dimensions
# MAGIC           depends_on:
# MAGIC             - task_key: silver_trips_enriched
# MAGIC             - task_key: bronze_zones
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../05-datamart/01_dm_dimensions.py
# MAGIC
# MAGIC         - task_key: dm_fact_trips          # termina con la conciliación (assert)
# MAGIC           depends_on:
# MAGIC             - task_key: dm_dimensions
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../05-datamart/02_dm_fact_trips.py
# MAGIC
# MAGIC         - task_key: dm_constraints_views
# MAGIC           depends_on:
# MAGIC             - task_key: dm_fact_trips
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../05-datamart/03_dm_constraints_views_grants.py
# MAGIC ```