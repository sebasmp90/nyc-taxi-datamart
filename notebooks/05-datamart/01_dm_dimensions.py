# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Data mart Finanzas TLC — 01 · Diseño y dimensiones
# MAGIC
# MAGIC Notebook 1 de 5 del data mart en modelo estrella (capa Gold, prefijo `gold_taxi_dm_`).
# MAGIC
# MAGIC | # | Notebook | Contenido |
# MAGIC |---|----------|-----------|
# MAGIC | 1 | `01_dm_dimensions` | **Parte 0** (diseño) y **Parte 1** (dimensiones) |
# MAGIC | 2 | `02_dm_fact_trips` | Parte 2: tabla de hechos, nulos y conciliación |
# MAGIC | 3 | `03_dm_constraints_views_grants` | Parte 3: PK/FK, vistas de consumo y permisos |
# MAGIC | 4 | `04_dm_business_questions` | Parte 4: preguntas de negocio solo con el mart |
# MAGIC | 5 | `05_dm_performance_orchestration` | Parte 5 (rendimiento y diseño físico) y Parte 6 (DAG) |
# MAGIC
# MAGIC Orden de ejecución: 1 → 2 → 3 → (4, 5). Requiere bronze y silver ya corridos.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
import datetime as dt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 0 · Diseño (antes de escribir código)
# MAGIC
# MAGIC ### 0.1 Grano de la tabla de hechos
# MAGIC
# MAGIC > **Cada fila de `gold_taxi_dm_fact_trips` es un viaje de taxi amarillo que pasó las reglas de calidad de silver**
# MAGIC > (una fila de `silver_yellow_trips_enriched`), descrito en el instante en que se encendió el taxímetro.
# MAGIC
# MAGIC El grano se fija primero porque decide qué llaves y medidas caben en la tabla: a nivel de viaje, cada viaje tiene
# MAGIC exactamente una fecha, una hora, una zona de origen, una de destino, una forma de pago, un tipo de tarifa y un proveedor.
# MAGIC Por eso las siete llaves foráneas son "naturales" y ninguna medida queda repetida ni partida.
# MAGIC
# MAGIC **¿Por qué no un grano agregado, por ejemplo "cada fila es una zona en un día"?** Al agregar se colapsan para siempre
# MAGIC las dimensiones que no están en el grano. Con `zona × día`:
# MAGIC
# MAGIC | Pregunta de la Parte 4 | Qué se pierde con `zona × día` |
# MAGIC |---|---|
# MAGIC | 2. Ticket de viajes que **terminan** en aeropuerto | La zona de **destino** (solo queda una zona por fila) |
# MAGIC | 3. Tarifa JFK que empieza **o** termina en JFK | El `RatecodeID` y el par origen–destino |
# MAGIC | 4. Propina por `day_part` y solo tarjeta | La **hora** y la **forma de pago** |
# MAGIC | 5. Mezcla de formas de pago | La **forma de pago** |
# MAGIC | 6. Peso de los recargos | Solo se salva si se guardó cada recargo como suma |
# MAGIC | Parte 5. Top pares origen → destino | El **par** origen–destino |
# MAGIC
# MAGIC Además, en un grano agregado los promedios ya calculados no se pueden volver a agregar (un promedio de promedios
# MAGIC está mal ponderado). Desde el grano de viaje siempre se puede agregar hacia arriba; desde un agregado nunca se
# MAGIC puede bajar. Si después se necesita un agregado por rendimiento, se construye **desde** esta tabla (ver Parte 5).
# MAGIC
# MAGIC ### 0.2 Diagrama del modelo estrella
# MAGIC
# MAGIC ```
# MAGIC                         ┌──────────────────────┐
# MAGIC                         │ gold_taxi_dm_dim_date│
# MAGIC                         │ PK date_key (yyyyMMdd)│
# MAGIC                         └──────────┬───────────┘
# MAGIC                                    │ pickup_date_key
# MAGIC  ┌────────────────────┐   ┌────────┴──────────────────┐   ┌──────────────────────┐
# MAGIC  │ dim_time           │   │ gold_taxi_dm_fact_trips   │   │ dim_payment_type     │
# MAGIC  │ PK hour_key (0–23) ├───┤ grano: 1 fila = 1 viaje   ├───┤ PK payment_type_key  │
# MAGIC  └────────────────────┘   │                           │   └──────────────────────┘
# MAGIC        pickup_hour_key    │ FK pickup_date_key        │      payment_type_key
# MAGIC                           │ FK pickup_hour_key        │
# MAGIC  ┌────────────────────┐   │ FK pickup_zone_key  ──┐   │   ┌──────────────────────┐
# MAGIC  │ dim_zone           │   │ FK dropoff_zone_key ──┤   ├───┤ dim_rate_code        │
# MAGIC  │ PK zone_key        ├───┤ FK payment_type_key   │   │   │ PK rate_code_key     │
# MAGIC  │ (role-playing:     │◄──┼───────────────────────┘   │   └──────────────────────┘
# MAGIC  │  pickup y dropoff) │   │ FK rate_code_key          │      rate_code_key
# MAGIC  └────────────────────┘   │ FK vendor_key             │   ┌──────────────────────┐
# MAGIC                           │ 13 medidas + 2 timestamps ├───┤ dim_vendor           │
# MAGIC                           └───────────────────────────┘   │ PK vendor_key        │
# MAGIC                                                           └──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ```mermaid
# MAGIC erDiagram
# MAGIC     FACT_TRIPS }o--|| DIM_DATE : pickup_date_key
# MAGIC     FACT_TRIPS }o--|| DIM_TIME : pickup_hour_key
# MAGIC     FACT_TRIPS }o--|| DIM_ZONE : pickup_zone_key
# MAGIC     FACT_TRIPS }o--|| DIM_ZONE : dropoff_zone_key
# MAGIC     FACT_TRIPS }o--|| DIM_PAYMENT_TYPE : payment_type_key
# MAGIC     FACT_TRIPS }o--|| DIM_RATE_CODE : rate_code_key
# MAGIC     FACT_TRIPS }o--|| DIM_VENDOR : vendor_key
# MAGIC ```
# MAGIC
# MAGIC ### 0.3 Dimensiones
# MAGIC
# MAGIC | Dimensión | Llave | Fuente | Atributos |
# MAGIC |---|---|---|---|
# MAGIC | `dim_date` | `date_key` INT `yyyyMMdd` | Generada con `sequence` + `explode` entre min y max de `pickup_date` en silver; festivos federales calculados por regla (5 U.S.C. § 6103) | `full_date`, `year`, `quarter`, `month`, `month_name`, `year_month`, `day_of_month`, `day_of_week` (1 = domingo), `day_name`, `is_weekend`, `is_holiday`, `holiday_name` |
# MAGIC | `dim_time` | `hour_key` 0–23 | Generada (24 filas) | `hour_label`, `day_part`, `day_part_order`, `is_rush_hour` |
# MAGIC | `dim_zone` | `zone_key` = `LocationID` | `T_BRONZE_ZONES` + miembro `-1` | `borough`, `zone`, `service_zone`, `is_airport`, `is_special_zone` |
# MAGIC | `dim_payment_type` | `payment_type_key` 0–6, `-1` | Data dictionary TLC | `payment_type_name`, `payment_type_desc_tlc`, `payment_group`, `tip_is_recorded` |
# MAGIC | `dim_rate_code` | `rate_code_key` 1–6, `-1` | Data dictionary TLC | `rate_code_name`, `rate_code_desc_tlc`, `is_flat_fare`, `is_airport_rate` |
# MAGIC | `dim_vendor` | `vendor_key` 1, 2, 6, 7, `-1` | Data dictionary TLC | `vendor_name`, `vendor_desc_tlc`, `is_accepted_by_silver` |
# MAGIC
# MAGIC `dim_zone` es una **role-playing dimension**: una sola tabla física que la tabla de hechos referencia dos veces
# MAGIC (`pickup_zone_key` y `dropoff_zone_key`). En las consultas se une dos veces con alias distintos (`pz`, `dz`).
# MAGIC
# MAGIC ### 0.4 Medidas
# MAGIC
# MAGIC | Medida | Tipo | Comentario |
# MAGIC |---|---|---|
# MAGIC | `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount` | **Aditiva** | Se suman por cualquier dimensión |
# MAGIC | `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee` | **Aditiva** (con `NULL` = no informado / no aplica) | `SUM` ignora los `NULL`; ver decisión de nulos en el notebook 02 |
# MAGIC | `trip_distance`, `trip_duration_min` | **Aditiva** | Millas y minutos totales recorridos; su promedio se calcula como `SUM / COUNT` |
# MAGIC | `passenger_count` | **Aditiva** | Pasajeros transportados; `NULL` en Flex Fare |
# MAGIC | `COUNT(*)` (viajes) | **Aditiva** | Medida implícita del grano |
# MAGIC | `tip_rate`, `cost_per_mile`, `avg_speed_mph` | **No aditivas** (ratios) | **No** se guardan en la tabla de hechos: se recalculan desde sus componentes |
# MAGIC
# MAGIC **Semi-aditivas:** en este mart no hay. Las semi-aditivas aparecen en tablas *snapshot* (saldos, inventarios,
# MAGIC "taxis activos al cierre del día"): se suman entre zonas pero no a lo largo del tiempo. Un viaje es un evento
# MAGIC transaccional, así que sus montos se suman en cualquier eje.
# MAGIC
# MAGIC ### Pregunta: ¿`tip_rate` y `avg_speed_mph` deben ir como medidas?
# MAGIC
# MAGIC **No.** Son ratios calculados por viaje y no son aditivos:
# MAGIC
# MAGIC - `SUM(tip_rate)` no significa nada (sumar porcentajes de viajes distintos).
# MAGIC - `AVG(tip_rate)` es un promedio **sin ponderar**: un viaje de USD 5 pesa lo mismo que uno de USD 90.
# MAGIC   Ejemplo: viaje A, tarifa 10 y propina 5 (50 %); viaje B, tarifa 90 y propina 9 (10 %).
# MAGIC   `AVG(tip_rate)` = 30 %, pero la tasa real de Finanzas es (5 + 9) / (10 + 90) = **14 %**.
# MAGIC - `AVG(avg_speed_mph)` igual: 1 milla en 2 min (30 mph) y 20 millas en 60 min (20 mph) dan 25 mph de promedio
# MAGIC   simple, cuando la velocidad real del conjunto es 21 millas / 62 min = **20,3 mph**.
# MAGIC
# MAGIC La forma correcta es guardar los componentes aditivos (`tip_amount`, `fare_amount`, `trip_distance`,
# MAGIC `trip_duration_min`) y recalcular el ratio en la consulta: `SUM(tip_amount) / SUM(fare_amount)`,
# MAGIC `SUM(trip_distance) / (SUM(trip_duration_min) / 60)`. Así cualquier corte (zona, hora, mes) da el ratio correcto.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparación: soltar constraints de una corrida anterior
# MAGIC
# MAGIC Si el notebook 03 ya corrió, las dimensiones tienen `PRIMARY KEY` y la tabla de hechos `FOREIGN KEY` hacia ellas.
# MAGIC Para poder reescribir las dimensiones de forma idempotente, primero se sueltan las FK y las PK; el notebook 03 las
# MAGIC vuelve a crear. Como en Databricks son informativas, no protegen nada mientras tanto: la integridad la garantiza
# MAGIC la conciliación del notebook 02.

# COMMAND ----------

FK_CONSTRAINTS = [
    "fk_fact_pickup_date", "fk_fact_pickup_hour", "fk_fact_pickup_zone", "fk_fact_dropoff_zone",
    "fk_fact_payment_type", "fk_fact_rate_code", "fk_fact_vendor",
]
DIM_TABLES = [T_DM_DIM_DATE, T_DM_DIM_TIME, T_DM_DIM_ZONE,
              T_DM_DIM_PAYMENT_TYPE, T_DM_DIM_RATE_CODE, T_DM_DIM_VENDOR]

if spark.catalog.tableExists(T_DM_FACT_TRIPS):
    for fk in FK_CONSTRAINTS:
        spark.sql(f"ALTER TABLE {T_DM_FACT_TRIPS} DROP CONSTRAINT IF EXISTS {fk}")
for dim in DIM_TABLES:
    if spark.catalog.tableExists(dim):
        spark.sql(f"ALTER TABLE {dim} DROP PRIMARY KEY IF EXISTS")
print("Constraints previas eliminadas (si existían).")


def save_dim(df, table, comment):
    """Escribe una dimensión como tabla administrada de UC y le pone comentario."""
    (df.write.mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table))
    spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")
    print(f"✓ {table}: {spark.table(table).count():,} filas")


def assert_unique_key(table, key):
    dups = spark.table(table).groupBy(key).count().filter("count > 1").count()
    nulls = spark.table(table).filter(F.col(key).isNull()).count()
    assert dups == 0, f"{table}: {dups} valores de {key} duplicados"
    assert nulls == 0, f"{table}: {nulls} valores de {key} nulos"
    print(f"✓ {table}: {key} único y no nulo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1.1 · `dim_date`
# MAGIC
# MAGIC Una fila por **día calendario** entre la fecha mínima y la máxima de `pickup_date` en silver, generada con
# MAGIC `sequence` + `explode` (no con `DISTINCT pickup_date`).
# MAGIC
# MAGIC **Festivos.** Feriados federales de EE. UU. según **5 U.S.C. § 6103** y el calendario de la **OPM**
# MAGIC (<https://www.opm.gov/policy-data-oversight/pay-leave/federal-holidays/>). En lugar de teclear una lista fija, se
# MAGIC calculan con las reglas de la ley para cualquier año del rango (así el mart no se rompe cuando se cargue 2026):
# MAGIC
# MAGIC | Festivo | Regla |
# MAGIC |---|---|
# MAGIC | New Year's Day | 1 de enero |
# MAGIC | Birthday of Martin Luther King, Jr. | 3.er lunes de enero |
# MAGIC | Washington's Birthday | 3.er lunes de febrero |
# MAGIC | Memorial Day | Último lunes de mayo |
# MAGIC | Juneteenth National Independence Day | 19 de junio (desde 2021) |
# MAGIC | Independence Day | 4 de julio |
# MAGIC | Labor Day | 1.er lunes de septiembre |
# MAGIC | Columbus Day | 2.º lunes de octubre |
# MAGIC | Veterans Day | 11 de noviembre |
# MAGIC | Thanksgiving Day | 4.º jueves de noviembre |
# MAGIC | Christmas Day | 25 de diciembre |
# MAGIC
# MAGIC Regla de **día observado** (OPM): si un festivo de fecha fija cae sábado se descansa el viernes anterior; si cae
# MAGIC domingo, el lunes siguiente. `is_holiday` marca el **día observado**, que es el día libre oficial; en ese caso
# MAGIC `holiday_name` termina en `(observado)`. Ejemplos que el notebook verifica: 2023-01-02 (Año Nuevo observado),
# MAGIC 2023-11-10 (Veterans Day observado), 2024-11-28 (Thanksgiving). No se incluye Inauguration Day: solo aplica a
# MAGIC empleados del área de Washington D. C.

# COMMAND ----------

silver = spark.table(T_SILVER_TRIPS_ENRICHED)
bounds = silver.agg(F.min(F.to_date("pickup_date")).alias("min_d"),
                    F.max(F.to_date("pickup_date")).alias("max_d")).first()
MIN_DATE, MAX_DATE = bounds["min_d"], bounds["max_d"]
print(f"Rango de silver: {MIN_DATE} → {MAX_DATE} ({(MAX_DATE - MIN_DATE).days + 1:,} días)")


def _nth_weekday(year, month, weekday, n):
    """n-ésimo día `weekday` (lunes=0) del mes."""
    first = dt.date(year, month, 1)
    return first + dt.timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(year, month, weekday):
    last = (dt.date(year + (month == 12), month % 12 + 1, 1) - dt.timedelta(days=1))
    return last - dt.timedelta(days=(last.weekday() - weekday) % 7)


def _observed(d):
    if d.weekday() == 5:   # sábado → viernes
        return d - dt.timedelta(days=1), True
    if d.weekday() == 6:   # domingo → lunes
        return d + dt.timedelta(days=1), True
    return d, False


def us_federal_holidays(year):
    MON, THU = 0, 3
    fixed = [(dt.date(year, 1, 1), "New Year's Day"),
             (dt.date(year, 7, 4), "Independence Day"),
             (dt.date(year, 11, 11), "Veterans Day"),
             (dt.date(year, 12, 25), "Christmas Day")]
    if year >= 2021:
        fixed.append((dt.date(year, 6, 19), "Juneteenth National Independence Day"))
    floating = [(_nth_weekday(year, 1, MON, 3), "Birthday of Martin Luther King, Jr."),
                (_nth_weekday(year, 2, MON, 3), "Washington's Birthday"),
                (_last_weekday(year, 5, MON), "Memorial Day"),
                (_nth_weekday(year, 9, MON, 1), "Labor Day"),
                (_nth_weekday(year, 10, MON, 2), "Columbus Day"),
                (_nth_weekday(year, 11, THU, 4), "Thanksgiving Day")]
    out = []
    for d, name in fixed:
        obs, moved = _observed(d)
        out.append((obs, name + (" (observado)" if moved else "")))
    return out + floating


# Un año antes y uno después: Año Nuevo de un sábado se observa el 31-dic del año anterior
holidays = [h for y in range(MIN_DATE.year - 1, MAX_DATE.year + 2)
            for h in us_federal_holidays(y) if MIN_DATE <= h[0] <= MAX_DATE]

# Control contra fechas publicadas por la OPM (solo las que caen en el rango cargado)
OPM_CHECK = {dt.date(2023, 1, 2): "New Year's Day (observado)",
             dt.date(2023, 11, 10): "Veterans Day (observado)",
             dt.date(2024, 11, 28): "Thanksgiving Day",
             dt.date(2025, 1, 20): "Birthday of Martin Luther King, Jr.",
             dt.date(2025, 5, 26): "Memorial Day"}
hol_dict = dict(holidays)
for d, name in OPM_CHECK.items():
    if MIN_DATE <= d <= MAX_DATE:
        assert hol_dict.get(d) == name, f"Festivo mal calculado en {d}: {hol_dict.get(d)}"
print(f"{len(holidays)} festivos federales en el rango")

holidays_df = spark.createDataFrame(
    holidays, T.StructType([T.StructField("full_date", T.DateType()),
                            T.StructField("holiday_name", T.StringType())]))

# COMMAND ----------

MESES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
         "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
DIAS = ["Domingo", "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado"]  # 1 = domingo

dim_date = (
    spark.range(1)
    .select(F.explode(F.sequence(F.lit(MIN_DATE), F.lit(MAX_DATE))).alias("full_date"))
    .join(holidays_df, "full_date", "left")
    .select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.element_at(F.array(*[F.lit(m) for m in MESES]), F.month("full_date")).alias("month_name"),
        F.date_format("full_date", "yyyy-MM").alias("year_month"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week"),          # 1 = domingo … 7 = sábado
        F.element_at(F.array(*[F.lit(d) for d in DIAS]), F.dayofweek("full_date")).alias("day_name"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
        F.col("holiday_name").isNotNull().alias("is_holiday"),
        "holiday_name",
    )
)

save_dim(dim_date, T_DM_DIM_DATE,
         "Dimensión calendario del data mart de Finanzas TLC. Una fila por día entre la primera y la última "
         "fecha de pickup en silver. Festivos federales de EE. UU. (5 U.S.C. 6103, día observado según OPM).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación `dim_date`

# COMMAND ----------

assert_unique_key(T_DM_DIM_DATE, "date_key")
dd = spark.table(T_DM_DIM_DATE)
expected_days = (MAX_DATE - MIN_DATE).days + 1
assert dd.count() == expected_days, f"Se esperaban {expected_days} días y hay {dd.count()}"

# Misma convención de día de la semana y fin de semana que silver
mismatch = (silver.select(F.to_date("pickup_date").alias("full_date"),
                          F.col("pickup_dayofweek").cast("int").alias("s_dow"),
                          F.col("is_weekend").cast("boolean").alias("s_we")).distinct()
            .join(dd, "full_date")
            .filter((F.col("s_dow") != F.col("day_of_week")) | (F.col("s_we") != F.col("is_weekend"))))
assert mismatch.count() == 0, "day_of_week / is_weekend no coinciden con silver"

dias_sin_viajes = dd.join(silver.select(F.to_date("pickup_date").alias("full_date")).distinct(),
                          "full_date", "left_anti").count()
print(f"✓ {expected_days:,} días | días del calendario sin ningún viaje en silver: {dias_sin_viajes}")
display(dd.filter("is_holiday").orderBy("full_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta: ¿qué se pierde si `dim_date` sale de los viajes y un día no tuvo ninguno?
# MAGIC
# MAGIC Ese día **desaparece del calendario**. Consecuencias:
# MAGIC
# MAGIC - Las series de tiempo quedan con huecos invisibles: un gráfico diario une el día anterior con el siguiente y nadie
# MAGIC   ve que hubo un día en cero (una tormenta, una caída del sistema del proveedor, un archivo que no se cargó).
# MAGIC - Los cálculos por calendario se dañan: promedio diario del mes (se divide entre menos días), ventanas móviles de
# MAGIC   7 días con `ROWS BETWEEN` (toman 7 filas, no 7 días), comparaciones año contra año.
# MAGIC - No se puede responder "¿qué días no tuvieron viajes?" con un `LEFT JOIN` desde el calendario: justamente el día
# MAGIC   que importa no existe.
# MAGIC - Un festivo sin viajes se pierde también como atributo.
# MAGIC - La dimensión deja de ser estable: cambia según los datos que lleguen, en vez de ser un calendario conformado que
# MAGIC   puede reutilizar cualquier otro mart.
# MAGIC
# MAGIC La celda anterior imprime cuántos días del calendario no tienen viajes en silver: con una dimensión derivada de los
# MAGIC viajes ese número sería siempre 0, por construcción, aunque el problema existiera.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1.2 · `dim_time`
# MAGIC
# MAGIC 24 filas, una por hora. `day_part`: madrugada 0–5, mañana 6–11, tarde 12–17, noche 18–23. `day_part_order`
# MAGIC permite ordenar las franjas en los reportes. `is_rush_hour` usa la **misma definición que silver**:
# MAGIC 7–9 h y 17–19 h (horas 7, 8, 9, 17, 18 y 19), y se verifica contra silver.

# COMMAND ----------

h = F.col("hour_key")
dim_time = (
    spark.range(24).select(F.col("id").cast("int").alias("hour_key"))
    .select(
        "hour_key",
        F.format_string("%02d:00–%02d:59", h, h).alias("hour_label"),
        F.when(h <= 5, "Madrugada").when(h <= 11, "Mañana")
         .when(h <= 17, "Tarde").otherwise("Noche").alias("day_part"),
        F.when(h <= 5, 1).when(h <= 11, 2).when(h <= 17, 3).otherwise(4).alias("day_part_order"),
        (h.between(7, 9) | h.between(17, 19)).alias("is_rush_hour"),
    )
)
save_dim(dim_time, T_DM_DIM_TIME,
         "Dimensión de hora del día (0-23) del data mart de Finanzas TLC. is_rush_hour = 7-9 h y 17-19 h, igual que silver.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación `dim_time` contra silver

# COMMAND ----------

assert_unique_key(T_DM_DIM_TIME, "hour_key")
dt_tbl = spark.table(T_DM_DIM_TIME)
assert dt_tbl.count() == 24

silver_rush = (silver.groupBy(F.col("pickup_hour").cast("int").alias("hour_key"))
               .agg(F.collect_set(F.col("is_rush_hour").cast("boolean")).alias("silver_rush")))
cmp = (silver_rush.join(dt_tbl.select("hour_key", "is_rush_hour"), "hour_key", "full")
       .withColumn("ok", (F.size("silver_rush") == 1) &
                   (F.element_at("silver_rush", 1) == F.col("is_rush_hour")))
       .orderBy("hour_key"))
display(cmp)
bad = cmp.filter(~F.col("ok") & F.col("silver_rush").isNotNull()).count()
assert bad == 0, (f"{bad} horas con is_rush_hour distinto a silver: revisa la definición en "
                  "02-silver/02_silver_trips_enriched y ajústala aquí")
print("✓ is_rush_hour coincide con silver en todas las horas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1.3 · `dim_zone` (role-playing)
# MAGIC
# MAGIC - Fuente: `T_BRONZE_ZONES` (`LocationID`, `Borough`, `Zone`, `service_zone`).
# MAGIC - `is_airport`: JFK, LaGuardia y Newark, identificados por su **nombre exacto** en el lookup (se verifica abajo).
# MAGIC - `is_special_zone`: `true` para 264 (`Unknown`) y 265 (`Outside of NYC`). **No se borran**: son viajes reales.
# MAGIC - Miembro `-1` (`Desconocido / No aplica`) por si algún viaje llega sin zona; también se marca como especial
# MAGIC   para que los filtros "sin zonas especiales" lo excluyan.
# MAGIC - Una sola tabla física: la tabla de hechos la usa como zona de pickup y como zona de dropoff.

# COMMAND ----------

AIRPORT_ZONES = ["JFK Airport", "LaGuardia Airport", "Newark Airport"]
SPECIAL_ZONE_IDS = [264, 265]

zones_src = (spark.table(T_BRONZE_ZONES)
             .select(F.col("LocationID").cast("int").alias("zone_key"),
                     F.trim("Borough").alias("borough"),
                     F.trim("Zone").alias("zone"),
                     F.trim("service_zone").alias("service_zone"))
             .distinct())   # bronze podría tener cargas repetidas idénticas

dup_zone = zones_src.groupBy("zone_key").count().filter("count > 1")
assert dup_zone.count() == 0, "bronze_taxi_zones tiene LocationID con atributos distintos: revisar fuente"

unknown_zone = spark.createDataFrame(
    [(-1, "Desconocido", "Desconocido / No aplica", "N/A")],
    "zone_key INT, borough STRING, zone STRING, service_zone STRING")

dim_zone = (
    zones_src.unionByName(unknown_zone)
    .select(
        "zone_key",
        F.coalesce("borough", F.lit("N/A")).alias("borough"),
        F.coalesce("zone", F.lit("N/A")).alias("zone"),
        F.coalesce("service_zone", F.lit("N/A")).alias("service_zone"),
        F.col("zone").isin(AIRPORT_ZONES).alias("is_airport"),
        (F.col("zone_key").isin(SPECIAL_ZONE_IDS) | (F.col("zone_key") == -1)).alias("is_special_zone"),
    )
)
save_dim(dim_zone, T_DM_DIM_ZONE,
         "Dimensión de zonas TLC (role-playing: pickup y dropoff). Incluye 264 Unknown y 265 Outside of NYC "
         "marcadas con is_special_zone, y el miembro -1 Desconocido / No aplica.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación `dim_zone`

# COMMAND ----------

assert_unique_key(T_DM_DIM_ZONE, "zone_key")
dz = spark.table(T_DM_DIM_ZONE)

airports = dz.filter("is_airport").select("zone_key", "borough", "zone", "service_zone").orderBy("zone_key")
display(airports)
assert sorted(r["zone"] for r in airports.collect()) == sorted(AIRPORT_ZONES), \
    "No se encontraron los 3 aeropuertos con esos nombres exactos: revisa el lookup"

# ¿Hay otras zonas con 'Airport' en el nombre que se nos estén escapando?
display(dz.filter(F.lower("zone").contains("airport")))
display(dz.filter("is_special_zone").orderBy("zone_key"))

# Todas las zonas usadas en silver deben existir en la dimensión
zonas_silver = (silver.select(F.col("PULocationID").cast("int").alias("zone_key"))
                .union(silver.select(F.col("DOLocationID").cast("int"))).distinct())
faltantes = zonas_silver.join(dz, "zone_key", "left_anti")
assert faltantes.count() == 0, f"Zonas de silver sin fila en dim_zone: {faltantes.collect()}"
print("✓ Todas las zonas de silver existen en dim_zone")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1.4 · `dim_payment_type`, `dim_rate_code`, `dim_vendor`
# MAGIC
# MAGIC Fuente: [Data Dictionary – Yellow Taxi Trip Records (TLC, 18-mar-2025)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).
# MAGIC
# MAGIC - `payment_type` 0–6. `tip_is_recorded = true` solo para tarjeta (1): el diccionario dice que `tip_amount` se llena
# MAGIC   automáticamente para propinas con tarjeta y que las propinas en efectivo no se incluyen. Con este atributo, la regla
# MAGIC   `payment_type = 1` vive en un solo lugar y las consultas filtran `WHERE tip_is_recorded`.
# MAGIC - `payment_group` agrupa para la mezcla de pagos: Tarjeta, Efectivo, Flex Fare y Otros.
# MAGIC - `RatecodeID` 1–6. El código 99 (`Null/unknown`) del diccionario no lo acepta silver; si apareciera iría a `-1`.
# MAGIC - `VendorID`: silver acepta 1 y 2. Se incluyen también 6 y 7 (están en el diccionario) con
# MAGIC   `is_accepted_by_silver = false`, para que la dimensión no haya que tocarla si silver los acepta mañana.
# MAGIC - Cada catálogo tiene el miembro `-1` = `Desconocido / No aplica`.

# COMMAND ----------

UNKNOWN = "Desconocido / No aplica"

dim_payment_type = spark.createDataFrame([
    (0,  "Flex Fare",           "Flex Fare trip",  "Flex Fare", False),
    (1,  "Tarjeta de crédito",  "Credit card",     "Tarjeta",   True),
    (2,  "Efectivo",            "Cash",            "Efectivo",  False),
    (3,  "Sin cargo",           "No charge",       "Otros",     False),
    (4,  "Disputa",             "Dispute",         "Otros",     False),
    (5,  "Desconocido (TLC)",   "Unknown",         "Otros",     False),
    (6,  "Viaje anulado",       "Voided trip",     "Otros",     False),
    (-1, UNKNOWN,               UNKNOWN,           "Otros",     False),
], "payment_type_key INT, payment_type_name STRING, payment_type_desc_tlc STRING, "
   "payment_group STRING, tip_is_recorded BOOLEAN")

dim_rate_code = spark.createDataFrame([
    (1,  "Tarifa estándar",          "Standard rate",          False, False),
    (2,  "JFK (tarifa fija)",        "JFK",                    True,  True),
    (3,  "Newark",                   "Newark",                 False, True),
    (4,  "Nassau o Westchester",     "Nassau or Westchester",  False, False),
    (5,  "Tarifa negociada",         "Negotiated fare",        True,  False),
    (6,  "Viaje grupal",             "Group ride",             False, False),
    (-1, UNKNOWN,                    UNKNOWN,                  False, False),
], "rate_code_key INT, rate_code_name STRING, rate_code_desc_tlc STRING, "
   "is_flat_fare BOOLEAN, is_airport_rate BOOLEAN")

dim_vendor = spark.createDataFrame([
    (1,  "Creative Mobile Technologies", "Creative Mobile Technologies, LLC", True),
    (2,  "Curb Mobility",                "Curb Mobility, LLC",                True),
    (6,  "Myle Technologies",            "Myle Technologies Inc",             False),
    (7,  "Helix",                        "Helix",                             False),
    (-1, UNKNOWN,                        UNKNOWN,                             False),
], "vendor_key INT, vendor_name STRING, vendor_desc_tlc STRING, is_accepted_by_silver BOOLEAN")

save_dim(dim_payment_type, T_DM_DIM_PAYMENT_TYPE,
         "Formas de pago TLC (data dictionary). tip_is_recorded = true solo para tarjeta. Miembro -1 = Desconocido / No aplica.")
save_dim(dim_rate_code, T_DM_DIM_RATE_CODE,
         "Tipos de tarifa TLC (RatecodeID). Flex Fare llega sin RatecodeID y apunta a -1.")
save_dim(dim_vendor, T_DM_DIM_VENDOR,
         "Proveedores de taxímetro TPEP (VendorID) según el data dictionary TLC.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación de catálogos

# COMMAND ----------

for table, key in [(T_DM_DIM_PAYMENT_TYPE, "payment_type_key"),
                   (T_DM_DIM_RATE_CODE, "rate_code_key"),
                   (T_DM_DIM_VENDOR, "vendor_key")]:
    assert_unique_key(table, key)
    assert spark.table(table).filter(F.col(key) == -1).count() == 1, f"{table} sin miembro -1"

# Cobertura de los códigos que trae silver (los no cubiertos irían a -1 en la tabla de hechos)
for src, table, key in [("payment_type", T_DM_DIM_PAYMENT_TYPE, "payment_type_key"),
                        ("RatecodeID", T_DM_DIM_RATE_CODE, "rate_code_key"),
                        ("VendorID", T_DM_DIM_VENDOR, "vendor_key")]:
    codes = silver.select(F.col(src).cast("int").alias(key)).where(F.col(key).isNotNull()).distinct()
    missing = [r[0] for r in codes.join(spark.table(table), key, "left_anti").collect()]
    assert not missing, f"Códigos de {src} en silver sin fila en {table}: {missing}"
    print(f"✓ {src}: todos los códigos de silver están en la dimensión")

assert [r[0] for r in spark.table(T_DM_DIM_PAYMENT_TYPE).filter("tip_is_recorded").collect()] == [1]
display(spark.table(T_DM_DIM_PAYMENT_TYPE).orderBy("payment_type_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta: ¿por qué un miembro `-1` es mejor que dejar la llave foránea en `NULL`?
# MAGIC
# MAGIC 1. **No se pierden filas.** `NULL = NULL` no es verdadero en SQL, así que un `INNER JOIN` con la dimensión descarta
# MAGIC    en silencio los viajes Flex Fare (sin `RatecodeID`). Con `-1` el join siempre encuentra pareja y los totales
# MAGIC    del mart cuadran con silver sin importar cómo escriba la consulta el analista.
# MAGIC 2. **Etiqueta legible.** En un reporte aparece "Desconocido / No aplica" en vez de un grupo en blanco o `null`
# MAGIC    que nadie sabe interpretar.
# MAGIC 3. **Integridad referencial.** Con `-1` cada llave foránea existe en su dimensión: se puede declarar la FK, la
# MAGIC    verificación de huérfanas es exacta (`left_anti` = 0) y las herramientas de BI pueden usar joins internos.
# MAGIC 4. **Semántica explícita.** `-1` dice "sabemos que este viaje no tiene tarifa"; un `NULL` no distingue entre
# MAGIC    "no aplica", "no llegó" o "hubo un error en el pipeline". Además, se distingue del código 5 (`Unknown`) que sí
# MAGIC    reporta la TLC.
# MAGIC 5. **Consultas simples.** No hace falta `COALESCE` ni `OR ... IS NULL` en cada filtro y `GROUP BY`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen

# COMMAND ----------

display(spark.createDataFrame(
    [(t.split(".")[-1].strip("`"), spark.table(t).count()) for t in DIM_TABLES],
    "tabla STRING, filas LONG"))