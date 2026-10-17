# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Data mart Finanzas TLC — 03 · Constraints, vistas y permisos
# MAGIC
# MAGIC **Parte 3.** `PRIMARY KEY` en cada dimensión, `FOREIGN KEY` desde la tabla de hechos, vistas de consumo para
# MAGIC Finanzas y permisos solo sobre las vistas.
# MAGIC
# MAGIC Requiere `01_dm_dimensions` y `02_dm_fact_trips` (con la conciliación en verde: las PK se declaran `RELY`
# MAGIC porque la conciliación ya probó que las llaves son únicas).

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Constraints
# MAGIC
# MAGIC Orden obligatorio:
# MAGIC
# MAGIC 1. Soltar FK previas de la tabla de hechos (una PK referenciada por una FK no se puede soltar).
# MAGIC 2. `ALTER COLUMN ... SET NOT NULL` en cada llave primaria (requisito de la `PRIMARY KEY`).
# MAGIC 3. `PRIMARY KEY ... RELY` en cada dimensión.
# MAGIC 4. `FOREIGN KEY` de la tabla de hechos hacia cada dimensión (la tabla referenciada ya debe tener su PK).
# MAGIC    `dim_zone` recibe **dos** FK: pickup y dropoff (role-playing).
# MAGIC
# MAGIC Todo usa `IF EXISTS` / `SET NOT NULL` idempotente para poder correr el notebook varias veces.

# COMMAND ----------

PKS = [  # (tabla, columna PK, nombre constraint)
    (T_DM_DIM_DATE, "date_key", "pk_dm_dim_date"),
    (T_DM_DIM_TIME, "hour_key", "pk_dm_dim_time"),
    (T_DM_DIM_ZONE, "zone_key", "pk_dm_dim_zone"),
    (T_DM_DIM_PAYMENT_TYPE, "payment_type_key", "pk_dm_dim_payment_type"),
    (T_DM_DIM_RATE_CODE, "rate_code_key", "pk_dm_dim_rate_code"),
    (T_DM_DIM_VENDOR, "vendor_key", "pk_dm_dim_vendor"),
]
FKS = [  # (nombre constraint, columna en fact, dimensión, PK)
    ("fk_fact_pickup_date", "pickup_date_key", T_DM_DIM_DATE, "date_key"),
    ("fk_fact_pickup_hour", "pickup_hour_key", T_DM_DIM_TIME, "hour_key"),
    ("fk_fact_pickup_zone", "pickup_zone_key", T_DM_DIM_ZONE, "zone_key"),
    ("fk_fact_dropoff_zone", "dropoff_zone_key", T_DM_DIM_ZONE, "zone_key"),
    ("fk_fact_payment_type", "payment_type_key", T_DM_DIM_PAYMENT_TYPE, "payment_type_key"),
    ("fk_fact_rate_code", "rate_code_key", T_DM_DIM_RATE_CODE, "rate_code_key"),
    ("fk_fact_vendor", "vendor_key", T_DM_DIM_VENDOR, "vendor_key"),
]

# 1. Soltar FK previas
for name, *_ in FKS:
    spark.sql(f"ALTER TABLE {T_DM_FACT_TRIPS} DROP CONSTRAINT IF EXISTS {name}")

# 2 y 3. NOT NULL + PRIMARY KEY RELY
for table, col, name in PKS:
    spark.sql(f"ALTER TABLE {table} DROP PRIMARY KEY IF EXISTS")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} SET NOT NULL")
    spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} PRIMARY KEY ({col}) RELY")
    print(f"✓ PK {name} ({col}) RELY")

# 4. FOREIGN KEY desde la tabla de hechos
for name, col, dim, pk in FKS:
    spark.sql(f"ALTER TABLE {T_DM_FACT_TRIPS} ADD CONSTRAINT {name} "
              f"FOREIGN KEY ({col}) REFERENCES {dim} ({pk})")
    print(f"✓ FK {name}: {col} → {dim.split('.')[-1].strip('`')}.{pk}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación en `information_schema`
# MAGIC
# MAGIC También se puede ver en **Catalog Explorer → tabla → pestaña Overview/Columns** (íconos de llave) y en el
# MAGIC diagrama de entidad-relación de la tabla de hechos.

# COMMAND ----------

constraints = spark.sql(f"""
    SELECT tc.table_name, tc.constraint_name, tc.constraint_type, kcu.column_name
    FROM `{CATALOG}`.information_schema.table_constraints tc
    JOIN `{CATALOG}`.information_schema.key_column_usage kcu
      ON  tc.constraint_catalog = kcu.constraint_catalog
      AND tc.constraint_schema  = kcu.constraint_schema
      AND tc.constraint_name    = kcu.constraint_name
    WHERE tc.table_schema = '{SCHEMA}'
      AND tc.table_name LIKE 'gold_taxi_dm_%'
    ORDER BY tc.constraint_type DESC, tc.table_name, tc.constraint_name
""")
display(constraints)

counts = {r["constraint_type"]: r["n"] for r in
          constraints.groupBy("constraint_type").agg(F.countDistinct("constraint_name").alias("n")).collect()}
assert counts.get("PRIMARY KEY") == len(PKS), f"PK registradas: {counts}"
assert counts.get("FOREIGN KEY") == len(FKS), f"FK registradas: {counts}"
print(f"✓ {len(PKS)} PRIMARY KEY y {len(FKS)} FOREIGN KEY registradas")

display(spark.sql(f"""
    SELECT constraint_name, unique_constraint_name
    FROM `{CATALOG}`.information_schema.referential_constraints
    WHERE constraint_schema = '{SCHEMA}' AND constraint_name LIKE 'fk_fact_%'
    ORDER BY constraint_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta: si las constraints son informativas, ¿para qué sirven? ¿Qué cambia con `RELY` y qué riesgo se asume?
# MAGIC
# MAGIC En Databricks las `PRIMARY KEY` y `FOREIGN KEY` **no se validan al escribir**: Delta acepta un duplicado o una
# MAGIC llave huérfana sin protestar (solo `NOT NULL` y `CHECK` se hacen cumplir). Aun así sirven para:
# MAGIC
# MAGIC - **Documentar el modelo** en el propio catálogo: cualquiera ve en Catalog Explorer cuál es la llave de cada
# MAGIC   dimensión y cómo se une con la tabla de hechos, sin leer el código.
# MAGIC - **Herramientas de consumo**: Power BI, Tableau, Genie / AI-BI y otros clientes leen las relaciones del
# MAGIC   `information_schema` para armar los joins correctos automáticamente.
# MAGIC - **Diagrama ER** generado en Catalog Explorer.
# MAGIC
# MAGIC **Con `RELY`** se le dice al optimizador "confía en que esta restricción se cumple", y entonces puede reescribir
# MAGIC consultas:
# MAGIC
# MAGIC - **Eliminar joins innecesarios**: un `LEFT JOIN dim_zone` del que no se usa ninguna columna se puede quitar,
# MAGIC   porque la unicidad de la PK garantiza que el join no multiplica filas.
# MAGIC - **Eliminar agregaciones redundantes**: un `DISTINCT` o `GROUP BY` sobre la PK no hace falta.
# MAGIC
# MAGIC **Riesgo:** si los datos violan la restricción (por ejemplo, una zona duplicada en `dim_zone`), el optimizador
# MAGIC igual aplica esas reescrituras y la consulta devuelve **resultados incorrectos sin ningún error**. Por eso aquí
# MAGIC `RELY` solo se declara en las PK **después** de que la conciliación del notebook 02 probó, con `assert`, que no hay
# MAGIC llaves duplicadas ni nulas, y esa conciliación corre en cada carga. `RELY` es una promesa: quien la declara
# MAGIC asume la responsabilidad de cumplirla.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Vistas de consumo para Finanzas
# MAGIC
# MAGIC Unen la estrella y exponen nombres legibles en español. Solo exponen **medidas aditivas** (sumas y conteos) para
# MAGIC que Finanzas pueda re-agregarlas sin error; los promedios y tasas se entregan junto con su numerador y
# MAGIC denominador.
# MAGIC
# MAGIC - **`gold_taxi_dm_v_revenue_mensual`**: año, mes, borough y `service_zone` de pickup, si es aeropuerto, viajes,
# MAGIC   revenue (`total_amount`), propinas y recargos. **Recargos** = `congestion_surcharge` + `airport_fee` +
# MAGIC   `cbd_congestion_fee` (la misma definición de la pregunta 6), con `COALESCE(…, 0)` porque aquí se suman.
# MAGIC   También se exponen por separado.
# MAGIC - **`gold_taxi_dm_v_propinas_tarjeta`**: propina promedio por borough de pickup, franja horaria y fin de semana,
# MAGIC   solo donde `tip_is_recorded` (la regla vive en `dim_payment_type`, no en la vista).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {T_DM_V_REVENUE_MENSUAL} (
    anio                COMMENT 'Año del pickup',
    mes                 COMMENT 'Mes del pickup (1-12)',
    nombre_mes          COMMENT 'Nombre del mes',
    anio_mes            COMMENT 'yyyy-MM',
    borough_pickup      COMMENT 'Borough de la zona de origen',
    service_zone_pickup COMMENT 'Service zone TLC de la zona de origen (Yellow Zone, Boro Zone, Airports, EWR)',
    es_aeropuerto_pickup COMMENT 'El viaje empezó en JFK, LaGuardia o Newark',
    viajes              COMMENT 'Número de viajes',
    revenue             COMMENT 'Suma de total_amount (USD, no incluye propinas en efectivo)',
    tarifa_base         COMMENT 'Suma de fare_amount (USD)',
    propinas            COMMENT 'Suma de tip_amount (USD; solo se registran con tarjeta)',
    recargo_congestion  COMMENT 'Suma de congestion_surcharge (USD)',
    recargo_aeropuerto  COMMENT 'Suma de airport_fee (USD)',
    recargo_cbd         COMMENT 'Suma de cbd_congestion_fee (USD; existe desde ene-2025)',
    recargos            COMMENT 'congestion + aeropuerto + CBD (USD)'
)
COMMENT 'Finanzas TLC: revenue, propinas y recargos por mes y zona de origen. Fuente: data mart gold_taxi_dm_.'
AS
SELECT d.year       AS anio,
       d.month      AS mes,
       d.month_name AS nombre_mes,
       d.year_month AS anio_mes,
       pz.borough      AS borough_pickup,
       pz.service_zone AS service_zone_pickup,
       pz.is_airport   AS es_aeropuerto_pickup,
       COUNT(*)                                       AS viajes,
       CAST(SUM(f.total_amount) AS DECIMAL(18,2))     AS revenue,
       CAST(SUM(f.fare_amount)  AS DECIMAL(18,2))     AS tarifa_base,
       CAST(SUM(f.tip_amount)   AS DECIMAL(18,2))     AS propinas,
       CAST(SUM(COALESCE(f.congestion_surcharge, 0)) AS DECIMAL(18,2)) AS recargo_congestion,
       CAST(SUM(COALESCE(f.airport_fee, 0))          AS DECIMAL(18,2)) AS recargo_aeropuerto,
       CAST(SUM(COALESCE(f.cbd_congestion_fee, 0))   AS DECIMAL(18,2)) AS recargo_cbd,
       CAST(SUM(COALESCE(f.congestion_surcharge, 0) + COALESCE(f.airport_fee, 0)
                + COALESCE(f.cbd_congestion_fee, 0)) AS DECIMAL(18,2)) AS recargos
FROM {T_DM_FACT_TRIPS} f
JOIN {T_DM_DIM_DATE} d  ON f.pickup_date_key = d.date_key
JOIN {T_DM_DIM_ZONE} pz ON f.pickup_zone_key = pz.zone_key
GROUP BY d.year, d.month, d.month_name, d.year_month, pz.borough, pz.service_zone, pz.is_airport
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {T_DM_V_PROPINAS_TARJETA} (
    borough_pickup      COMMENT 'Borough de la zona de origen',
    franja_horaria      COMMENT 'Madrugada (0-5), Mañana (6-11), Tarde (12-17), Noche (18-23)',
    orden_franja        COMMENT 'Para ordenar las franjas (1-4)',
    es_fin_de_semana    COMMENT 'Sábado o domingo',
    viajes_tarjeta      COMMENT 'Viajes pagados con tarjeta (única forma de pago con propina registrada)',
    propinas_total      COMMENT 'Suma de tip_amount (USD)',
    tarifa_total        COMMENT 'Suma de fare_amount (USD)',
    propina_promedio    COMMENT 'Propina promedio por viaje (USD) = propinas_total / viajes_tarjeta',
    tasa_propina        COMMENT 'propinas_total / tarifa_total (recalculada, no promedio de ratios)'
)
COMMENT 'Finanzas TLC: propinas con tarjeta por borough, franja horaria y fin de semana. Solo payment types con tip_is_recorded.'
AS
SELECT pz.borough        AS borough_pickup,
       t.day_part        AS franja_horaria,
       t.day_part_order  AS orden_franja,
       d.is_weekend      AS es_fin_de_semana,
       COUNT(*)                                     AS viajes_tarjeta,
       CAST(SUM(f.tip_amount)  AS DECIMAL(18,2))    AS propinas_total,
       CAST(SUM(f.fare_amount) AS DECIMAL(18,2))    AS tarifa_total,
       CAST(AVG(f.tip_amount)  AS DECIMAL(10,4))    AS propina_promedio,
       CAST(SUM(f.tip_amount) / NULLIF(SUM(f.fare_amount), 0) AS DECIMAL(10,4)) AS tasa_propina
FROM {T_DM_FACT_TRIPS} f
JOIN {T_DM_DIM_PAYMENT_TYPE} p ON f.payment_type_key = p.payment_type_key
JOIN {T_DM_DIM_DATE} d         ON f.pickup_date_key  = d.date_key
JOIN {T_DM_DIM_TIME} t         ON f.pickup_hour_key  = t.hour_key
JOIN {T_DM_DIM_ZONE} pz        ON f.pickup_zone_key  = pz.zone_key
WHERE p.tip_is_recorded
GROUP BY pz.borough, t.day_part, t.day_part_order, d.is_weekend
""")
print("✓ Vistas creadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación de las vistas
# MAGIC
# MAGIC Las vistas deben cuadrar con la tabla de hechos: viajes y revenue totales de `v_revenue_mensual` = tabla de
# MAGIC hechos; viajes de `v_propinas_tarjeta` = viajes con `payment_type_key = 1`.

# COMMAND ----------

v_rev = spark.table(T_DM_V_REVENUE_MENSUAL)
v_tip = spark.table(T_DM_V_PROPINAS_TARJETA)
ft = spark.table(T_DM_FACT_TRIPS)

tot_v = v_rev.agg(F.sum("viajes").alias("n"), F.sum("revenue").alias("rev")).first()
tot_f = ft.agg(F.count("*").alias("n"),
               F.sum(F.col("total_amount").cast("decimal(38,2)")).alias("rev")).first()
assert tot_v["n"] == tot_f["n"], f"Viajes vista {tot_v['n']} ≠ fact {tot_f['n']}"
assert abs(float(tot_v["rev"]) - float(tot_f["rev"])) < 1.0, f"Revenue vista {tot_v['rev']} ≠ fact {tot_f['rev']}"

n_card = ft.filter("payment_type_key = 1").count()
assert v_tip.agg(F.sum("viajes_tarjeta")).first()[0] == n_card
print(f"✓ v_revenue_mensual: {tot_v['n']:,} viajes / USD {tot_v['rev']:,} | v_propinas_tarjeta: {n_card:,} viajes")

display(v_rev.orderBy("anio", "mes", F.desc("revenue")).limit(20))
display(v_tip.orderBy("borough_pickup", "orden_franja", "es_fin_de_semana"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Permisos (en parejas)
# MAGIC
# MAGIC Escribe el correo de tu compañero en el widget `companero` (arriba del notebook) y corre las celdas. Se otorga:
# MAGIC
# MAGIC - `USE SCHEMA` sobre tu schema (el `USE CATALOG` sobre el catálogo del curso ya lo tiene).
# MAGIC - `SELECT` **solo sobre las dos vistas**. Nada sobre `fact_trips`, las dimensiones ni silver.
# MAGIC
# MAGIC Tu compañero corre el notebook `03b_dm_partner_test` con **tu** schema en su widget y te pasa la salida.
# MAGIC Si el profesor otorgó `SELECT` a nivel de catálogo a todos los estudiantes, tu compañero sí podría leer
# MAGIC `fact_trips` por herencia: en ese caso la evidencia de `SHOW GRANTS` lo deja ver y hay que mencionarlo.

# COMMAND ----------

dbutils.widgets.text("companero", "", "Correo del compañero")
PARTNER = dbutils.widgets.get("companero").strip()

VIEWS = [T_DM_V_REVENUE_MENSUAL, T_DM_V_PROPINAS_TARJETA]

if not PARTNER:
    print("⚠ Escribe el correo de tu compañero en el widget 'companero' y vuelve a correr esta celda.")
else:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{CATALOG}`.`{SCHEMA}` TO `{PARTNER}`")
    for v in VIEWS:
        spark.sql(f"GRANT SELECT ON TABLE {v} TO `{PARTNER}`")
    print(f"✓ {PARTNER}: USE SCHEMA en {SCHEMA} y SELECT en {len(VIEWS)} vistas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidencia: `SHOW GRANTS`

# COMMAND ----------

if PARTNER:
    print("── Grants del compañero sobre el schema")
    display(spark.sql(f"SHOW GRANTS `{PARTNER}` ON SCHEMA `{CATALOG}`.`{SCHEMA}`"))
    for v in VIEWS:
        print(f"── Grants sobre {v}")
        display(spark.sql(f"SHOW GRANTS ON TABLE {v}"))
    for t in [T_DM_FACT_TRIPS, T_SILVER_TRIPS_ENRICHED]:
        print(f"── Grants del compañero sobre {t} (debe salir vacío)")
        display(spark.sql(f"SHOW GRANTS `{PARTNER}` ON TABLE {t}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidencia del compañero
# MAGIC
# MAGIC Pega aquí la salida del notebook `03b_dm_partner_test` que corrió tu compañero:
# MAGIC
# MAGIC ```
# MAGIC ✓ <vista revenue_mensual>: N filas
# MAGIC ✓ <vista propinas_tarjeta>: N filas
# MAGIC ✗ <fact_trips>: [INSUFFICIENT_PERMISSIONS] Insufficient privileges: User does not have SELECT on Table ...
# MAGIC ✗ <silver_yellow_trips_enriched>: [INSUFFICIENT_PERMISSIONS] ...
# MAGIC ```
# MAGIC
# MAGIC ### Pregunta: ¿por qué tu compañero lee la vista aunque no tenga `SELECT` sobre las tablas que consulta?
# MAGIC
# MAGIC Porque en Unity Catalog una vista se ejecuta con los **privilegios de su dueño** (*definer's rights* /
# MAGIC *ownership chaining*). Al consultar la vista se verifican dos cosas distintas:
# MAGIC
# MAGIC 1. Que **quien consulta** tenga `SELECT` sobre la vista (y `USE CATALOG` / `USE SCHEMA` para llegar a ella).
# MAGIC 2. Que **el dueño de la vista** (tú) tenga `SELECT` sobre las tablas que la vista lee.
# MAGIC
# MAGIC Tu compañero nunca toca `fact_trips` ni las dimensiones directamente: la vista es la "ventana" que tú decidiste
# MAGIC abrir, con las columnas y el nivel de agregación que elegiste. Así se cumple el requisito de que Finanzas **no
# MAGIC tenga acceso a silver** y que solo vea datos agregados y legibles. Si tú perdieras el `SELECT` sobre la tabla de
# MAGIC hechos, la vista dejaría de funcionar para todos.