# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Data mart Finanzas TLC — 02 · Tabla de hechos `fact_trips`
# MAGIC
# MAGIC **Parte 2.** Grano: **un viaje** (una fila de `silver_yellow_trips_enriched`). La tabla debe tener exactamente las
# MAGIC mismas filas que silver, siete llaves foráneas, trece medidas y ningún atributo descriptivo.
# MAGIC
# MAGIC Requiere haber corrido `01_dm_dimensions`.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Llaves foráneas
# MAGIC
# MAGIC - `pickup_date_key` = `yyyyMMdd` de `pickup_date`; `pickup_hour_key` = `pickup_hour`. `dim_date` y `dim_time`
# MAGIC   cubren por construcción todo el rango, así que no necesitan miembro `-1` (la conciliación lo verifica).
# MAGIC - Zonas, forma de pago, tarifa y proveedor: **`left join` con `F.broadcast(dim)`** sobre el código y
# MAGIC   `coalesce(llave_dim, -1)`. Un `inner join` descartaría en silencio los viajes sin correspondencia (por ejemplo,
# MAGIC   Flex Fare sin `RatecodeID`) y la conciliación de conteo fallaría.
# MAGIC - `dim_zone` se une **dos veces** (role-playing): una para pickup y otra para dropoff.
# MAGIC - Las dimensiones son diminutas (≤ 266 filas), por eso se difunden (*broadcast*) a todos los ejecutores y el join
# MAGIC   no provoca *shuffle* de los viajes.

# COMMAND ----------

silver = spark.table(T_SILVER_TRIPS_ENRICHED)


def with_dim_key(df, src_col, dim_table, dim_key, out_col):
    """left join broadcast contra la dimensión; si no hay pareja (o el código es NULL) → -1."""
    tmp = f"__{out_col}"
    keys = (spark.table(dim_table)
            .select(F.col(dim_key).alias(tmp))
            .where(F.col(tmp) != -1))
    return (df.join(F.broadcast(keys), F.col(src_col).cast("int") == F.col(tmp), "left")
              .withColumn(out_col, F.coalesce(F.col(tmp), F.lit(-1)).cast("int"))
              .drop(tmp))


fact = silver.withColumns({
    "pickup_date_key": F.date_format(F.to_date("pickup_date"), "yyyyMMdd").cast("int"),
    "pickup_hour_key": F.col("pickup_hour").cast("int"),
})
fact = with_dim_key(fact, "PULocationID", T_DM_DIM_ZONE, "zone_key", "pickup_zone_key")
fact = with_dim_key(fact, "DOLocationID", T_DM_DIM_ZONE, "zone_key", "dropoff_zone_key")
fact = with_dim_key(fact, "payment_type", T_DM_DIM_PAYMENT_TYPE, "payment_type_key", "payment_type_key")
fact = with_dim_key(fact, "RatecodeID", T_DM_DIM_RATE_CODE, "rate_code_key", "rate_code_key")
fact = with_dim_key(fact, "VendorID", T_DM_DIM_VENDOR, "vendor_key", "vendor_key")

FK_COLS = ["pickup_date_key", "pickup_hour_key", "pickup_zone_key", "dropoff_zone_key",
           "payment_type_key", "rate_code_key", "vendor_key"]
MEASURES = ["passenger_count", "trip_distance", "trip_duration_min",
            "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
            "improvement_surcharge", "congestion_surcharge", "airport_fee",
            "cbd_congestion_fee", "total_amount"]
TRACE = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

fact = fact.select(*FK_COLS, *MEASURES, *TRACE)
fact.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Nulos en las medidas: decisión y justificación
# MAGIC
# MAGIC **Decisión: las medidas faltantes quedan en `NULL`, no en `0`.**
# MAGIC
# MAGIC | Medida | ¿Cuándo falta? | Significado real |
# MAGIC |---|---|---|
# MAGIC | `passenger_count`, `congestion_surcharge`, `airport_fee` | Flex Fare (`payment_type = 0`) | **No informado**: la TLC no publica el dato; no sabemos si fue 0 |
# MAGIC | `cbd_congestion_fee` | Antes del 5-ene-2025 | **No aplica**: el cargo no existía |
# MAGIC
# MAGIC Un `0` afirma "se cobró cero", que es falso para Flex Fare (simplemente no lo sabemos) e induce a error en los
# MAGIC promedios:
# MAGIC
# MAGIC - `SUM(...)` da **igual** con `NULL` o con `0`: los totales de Finanzas no cambian.
# MAGIC - `AVG(congestion_surcharge)` con **`NULL`** = suma / viajes **con dato**: el recargo promedio real de los viajes
# MAGIC   que sí lo reportan.
# MAGIC - `AVG(congestion_surcharge)` con **`0`** = suma / **todos** los viajes: el promedio queda sesgado hacia abajo en
# MAGIC   proporción a la participación de Flex Fare, y el sesgo cambia mes a mes según cuánto Flex Fare haya.
# MAGIC - Igual con `cbd_congestion_fee`: con `NULL`, el promedio de 2024 es `NULL` ("no existía"); con `0` diría que en
# MAGIC   2024 se cobró en promedio USD 0, y un promedio 2024–2025 mezclaría ambos periodos.
# MAGIC - `passenger_count` con `0` reduciría los pasajeros por viaje y contaría viajes "sin pasajeros" que no existieron.
# MAGIC
# MAGIC Quien necesite tratar el faltante como cero (por ejemplo, para sumar recargos fila a fila) lo hace explícito con
# MAGIC `COALESCE(x, 0)` en la consulta o en la vista, como en `gold_taxi_dm_v_revenue_mensual`. La celda siguiente
# MAGIC muestra el efecto con los datos reales.
# MAGIC
# MAGIC Nota: la tabla de hechos copia los valores tal como vienen de silver. Si silver ya hubiera convertido estos `NULL`
# MAGIC en `0`, la celda siguiente lo dejaría en evidencia (0 nulos en Flex Fare).

# COMMAND ----------

nulls_by_payment = (
    fact.groupBy("payment_type_key")
    .agg(F.count("*").alias("viajes"),
         *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
           for c in ["passenger_count", "congestion_surcharge", "airport_fee", "cbd_congestion_fee"]],
         F.round(F.avg("congestion_surcharge"), 4).alias("avg_congestion_con_NULL"),
         F.round(F.avg(F.coalesce("congestion_surcharge", F.lit(0.0))), 4).alias("avg_congestion_con_0"))
    .orderBy("payment_type_key")
)
display(nulls_by_payment)

display(fact.agg(
    F.round(F.avg("congestion_surcharge"), 4).alias("avg_congestion_NULL_total"),
    F.round(F.avg(F.coalesce("congestion_surcharge", F.lit(0.0))), 4).alias("avg_congestion_0_total"),
    F.round(F.avg("cbd_congestion_fee"), 4).alias("avg_cbd_NULL"),
    F.round(F.avg(F.coalesce("cbd_congestion_fee", F.lit(0.0))), 4).alias("avg_cbd_0"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Escritura
# MAGIC
# MAGIC Se crea con `CREATE OR REPLACE TABLE ... CLUSTER BY (pickup_date_key, pickup_zone_key)` (liquid clustering).
# MAGIC La justificación del diseño físico está en el notebook 05 (Parte 5.3). `CREATE OR REPLACE` conserva el historial
# MAGIC Delta de la tabla y deja la escritura idempotente. Luego `OPTIMIZE` agrupa los archivos según las llaves.

# COMMAND ----------

fact.createOrReplaceTempView("_stage_fact_trips")
spark.sql(f"""
    CREATE OR REPLACE TABLE {T_DM_FACT_TRIPS}
    CLUSTER BY (pickup_date_key, pickup_zone_key)
    COMMENT 'Hechos de viajes del data mart de Finanzas TLC. Grano: un viaje (una fila de silver_yellow_trips_enriched). Medidas faltantes en NULL = no informado / no aplica.'
    AS SELECT * FROM _stage_fact_trips
""")
spark.sql(f"OPTIMIZE {T_DM_FACT_TRIPS}")
print(f"✓ {T_DM_FACT_TRIPS} escrita")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación rápida

# COMMAND ----------

ft = spark.table(T_DM_FACT_TRIPS)
display(ft.limit(10))

# Ningún atributo descriptivo en la tabla de hechos
forbidden = [c for c in ft.columns
             if any(w in c.lower() for w in ["borough", "zone_name", "service_zone", "_desc", "_name"])
             or c in ("pickup_zone", "dropoff_zone")]
assert not forbidden, f"Atributos descriptivos en la tabla de hechos: {forbidden}"

# Distribución de miembros -1 por llave (Flex Fare → rate_code_key = -1)
display(ft.select(*[F.sum((F.col(k) == -1).cast("int")).alias(k) for k in FK_COLS]))
display(ft.groupBy("payment_type_key")
          .agg(F.count("*").alias("viajes"),
               F.sum((F.col("rate_code_key") == -1).cast("int")).alias("rate_code_-1"))
          .orderBy("payment_type_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Conciliación (obligatoria)
# MAGIC
# MAGIC Esta celda **lanza `AssertionError`** si algo no cuadra, para que la tarea del workflow falle:
# MAGIC
# MAGIC 1. `COUNT(*)` de la tabla de hechos = `COUNT(*)` de silver.
# MAGIC 2. `SUM(total_amount)` de la tabla de hechos = `SUM(total_amount)` de silver. Se suma como `DECIMAL(38,2)` para
# MAGIC    que la comparación sea exacta (sumar `DOUBLE` en distinto orden puede diferir en el último decimal binario).
# MAGIC 3. Cero llaves huérfanas: cada llave foránea existe en su dimensión (`left_anti` join).
# MAGIC 4. Cero llaves duplicadas (y cero nulas) en cada dimensión.
# MAGIC
# MAGIC Además se verifica que no haya códigos de silver que hayan caído en `-1` sin ser `NULL`, lo que indicaría un
# MAGIC código nuevo que la dimensión no conoce.

# COMMAND ----------

ft = spark.table(T_DM_FACT_TRIPS)
silver = spark.table(T_SILVER_TRIPS_ENRICHED)
results = []   # (chequeo, esperado, obtenido, ok)

# 1. Conteo
n_silver, n_fact = silver.count(), ft.count()
results.append(("1. COUNT(*) fact = silver", n_silver, n_fact, n_silver == n_fact))

# 2. Suma de total_amount
def exact_sum(df):
    return df.agg(F.sum(F.col("total_amount").cast("decimal(38,2)"))).first()[0]
s_silver, s_fact = exact_sum(silver), exact_sum(ft)
results.append(("2. SUM(total_amount) fact = silver", s_silver, s_fact, s_silver == s_fact))

# 3. Huérfanas
FK_MAP = [("pickup_date_key", T_DM_DIM_DATE, "date_key"),
          ("pickup_hour_key", T_DM_DIM_TIME, "hour_key"),
          ("pickup_zone_key", T_DM_DIM_ZONE, "zone_key"),
          ("dropoff_zone_key", T_DM_DIM_ZONE, "zone_key"),
          ("payment_type_key", T_DM_DIM_PAYMENT_TYPE, "payment_type_key"),
          ("rate_code_key", T_DM_DIM_RATE_CODE, "rate_code_key"),
          ("vendor_key", T_DM_DIM_VENDOR, "vendor_key")]
for fk, dim, pk in FK_MAP:
    orphans = (ft.select(F.col(fk).alias("k"))
                 .join(F.broadcast(spark.table(dim).select(F.col(pk).alias("k"))), "k", "left_anti")
                 .count())
    results.append((f"3. Huérfanas {fk} → {dim.split('.')[-1].strip('`')}", 0, orphans, orphans == 0))

# 4. Duplicados y nulos en las llaves de las dimensiones
for dim, pk in sorted({(d, p) for _, d, p in FK_MAP}):
    d = spark.table(dim)
    dups = d.groupBy(pk).count().filter("count > 1").count()
    nulls = d.filter(F.col(pk).isNull()).count()
    results.append((f"4. Duplicadas/nulas {pk} en {dim.split('.')[-1].strip('`')}", 0, dups + nulls,
                    dups + nulls == 0))

# Extra: códigos no NULL de silver que cayeron en -1 (código nuevo desconocido para la dimensión)
src_keys = silver.select(
    F.col("PULocationID").cast("int").alias("PULocationID"), F.col("DOLocationID").cast("int").alias("DOLocationID"),
    F.col("payment_type").cast("int").alias("payment_type"), F.col("RatecodeID").cast("int").alias("RatecodeID"),
    F.col("VendorID").cast("int").alias("VendorID"))
for src, dim, pk in [("PULocationID", T_DM_DIM_ZONE, "zone_key"), ("DOLocationID", T_DM_DIM_ZONE, "zone_key"),
                     ("payment_type", T_DM_DIM_PAYMENT_TYPE, "payment_type_key"),
                     ("RatecodeID", T_DM_DIM_RATE_CODE, "rate_code_key"),
                     ("VendorID", T_DM_DIM_VENDOR, "vendor_key")]:
    unknown = (src_keys.where(F.col(src).isNotNull()).select(F.col(src).alias("k"))
               .join(F.broadcast(spark.table(dim).select(F.col(pk).alias("k"))), "k", "left_anti").count())
    results.append((f"5. Códigos {src} no reconocidos (→ -1)", 0, unknown, unknown == 0))

report = spark.createDataFrame(
    [(c, str(e), str(o), ok) for c, e, o, ok in results],
    "chequeo STRING, esperado STRING, obtenido STRING, ok BOOLEAN")
display(report)

failed = [r for r in results if not r[3]]
if failed:
    raise AssertionError("Conciliación FALLIDA:\n" + "\n".join(
        f"  ✗ {c}: esperado {e}, obtenido {o}" for c, e, o, _ in failed))
print(f"✓ Conciliación OK: {n_fact:,} viajes, total_amount = {s_fact:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta: ¿por qué la conciliación debe hacer fallar el notebook en lugar de solo mostrar los números?
# MAGIC
# MAGIC A las 3 a. m. **nadie está mirando la salida de la celda**. Si la conciliación solo imprime, el notebook termina
# MAGIC "en verde" aunque falten 200 000 viajes o haya llaves huérfanas, y:
# MAGIC
# MAGIC - Las tareas siguientes del workflow (constraints, vistas, dashboards) corren sobre datos malos y Finanzas abre
# MAGIC   a las 8 a. m. un reporte con cifras equivocadas que nadie marcó.
# MAGIC - El error se descubre días después, cuando ya se tomaron decisiones con esos números y hay que rastrear cuándo
# MAGIC   empezó.
# MAGIC
# MAGIC Si la celda lanza una excepción:
# MAGIC
# MAGIC - La tarea queda en **Failed**, las tareas dependientes **no se ejecutan** y lo publicado ayer sigue siendo lo
# MAGIC   último bueno (fallar es mejor que publicar mal).
# MAGIC - Se disparan las **notificaciones** del job (correo, Slack) y, si se configuraron, los **reintentos**.
# MAGIC - Queda registro en el historial de corridas con el mensaje exacto del chequeo que falló.
# MAGIC
# MAGIC Es el principio de *fail fast*: la calidad del dato se convierte en una condición de éxito del pipeline y no en
# MAGIC un log que alguien tiene que acordarse de leer.