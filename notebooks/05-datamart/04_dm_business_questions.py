# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Data mart Finanzas TLC — 04 · Preguntas de negocio
# MAGIC
# MAGIC **Parte 4.** Todas las respuestas usan **solo el mart** (vistas o joins sobre la estrella). Ninguna consulta toca
# MAGIC silver: es la prueba de que el modelo responde preguntas nuevas sin pedirle a ingeniería otra tabla.
# MAGIC
# MAGIC Cada pregunta tiene: la consulta, una celda que imprime la respuesta con las cifras reales y una celda `%md` con
# MAGIC la interpretación. Las celdas `%md` explican **qué mirar y por qué**; las cifras concretas salen de la celda
# MAGIC anterior (completa los `____` con tu salida antes de entregar).

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# Alias cortos para las consultas SQL
FACT, D_DATE, D_TIME, D_ZONE = T_DM_FACT_TRIPS, T_DM_DIM_DATE, T_DM_DIM_TIME, T_DM_DIM_ZONE
D_PAY, D_RATE, D_VEND = T_DM_DIM_PAYMENT_TYPE, T_DM_DIM_RATE_CODE, T_DM_DIM_VENDOR
V_REV, V_TIP = T_DM_V_REVENUE_MENSUAL, T_DM_V_PROPINAS_TARJETA

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Revenue mensual por borough de pickup y `service_zone`. ¿Qué participación tienen los aeropuertos?
# MAGIC
# MAGIC Se usa la vista `v_revenue_mensual` (la misma que consume Finanzas). Como la vista solo expone sumas, se puede
# MAGIC re-agregar sin error. La participación de aeropuertos se mide de dos formas:
# MAGIC
# MAGIC - **Pickup en aeropuerto** (desde la vista): ingresos de viajes que **salen** de JFK, LaGuardia o Newark.
# MAGIC - **Toca un aeropuerto** (desde la estrella): viajes que salen **o** llegan a un aeropuerto, con la role-playing
# MAGIC   dimension `dim_zone` unida dos veces.

# COMMAND ----------

q1 = spark.sql(f"""
SELECT anio, mes, anio_mes, borough_pickup, service_zone_pickup,
       SUM(viajes)  AS viajes,
       SUM(revenue) AS revenue,
       ROUND(100 * SUM(revenue) / SUM(SUM(revenue)) OVER (PARTITION BY anio_mes), 2) AS pct_revenue_mes
FROM {V_REV}
GROUP BY anio, mes, anio_mes, borough_pickup, service_zone_pickup
ORDER BY anio, mes, revenue DESC
""")
display(q1)

# COMMAND ----------

q1_air = spark.sql(f"""
WITH pick AS (
  SELECT anio_mes,
         SUM(revenue) AS revenue_total,
         SUM(CASE WHEN es_aeropuerto_pickup THEN revenue ELSE 0 END) AS revenue_pickup_aeropuerto,
         SUM(viajes) AS viajes_total,
         SUM(CASE WHEN es_aeropuerto_pickup THEN viajes ELSE 0 END) AS viajes_pickup_aeropuerto
  FROM {V_REV} GROUP BY anio_mes
),
toca AS (
  SELECT d.year_month AS anio_mes,
         SUM(CASE WHEN pz.is_airport OR dz.is_airport THEN f.total_amount ELSE 0 END) AS revenue_toca_aeropuerto
  FROM {FACT} f
  JOIN {D_DATE} d  ON f.pickup_date_key  = d.date_key
  JOIN {D_ZONE} pz ON f.pickup_zone_key  = pz.zone_key
  JOIN {D_ZONE} dz ON f.dropoff_zone_key = dz.zone_key
  GROUP BY d.year_month
)
SELECT p.anio_mes,
       ROUND(100 * viajes_pickup_aeropuerto  / viajes_total, 2)  AS pct_viajes_pickup_aeropuerto,
       ROUND(100 * revenue_pickup_aeropuerto / revenue_total, 2) AS pct_revenue_pickup_aeropuerto,
       ROUND(100 * revenue_toca_aeropuerto   / revenue_total, 2) AS pct_revenue_toca_aeropuerto
FROM pick p JOIN toca t USING (anio_mes)
ORDER BY p.anio_mes
""")
display(q1_air)

tot = q1_air.agg(F.avg("pct_viajes_pickup_aeropuerto").alias("v"),
                 F.avg("pct_revenue_pickup_aeropuerto").alias("r"),
                 F.avg("pct_revenue_toca_aeropuerto").alias("t")).first()
print(f"Promedio mensual: los pickups en aeropuerto son el {tot['v']:.1f}% de los viajes pero el {tot['r']:.1f}% "
      f"del revenue; los viajes que tocan un aeropuerto (origen o destino) suman el {tot['t']:.1f}% del revenue.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Respuesta 1.** Manhattan (`Yellow Zone`) concentra la mayor parte de viajes y revenue. Los aeropuertos aparecen
# MAGIC en `service_zone = 'Airports'` (JFK y LaGuardia, borough Queens) y `EWR` (Newark). Su peso en **revenue** es varias
# MAGIC veces mayor que su peso en **viajes**: son pocos viajes pero largos, con peajes, `airport_fee` y, en JFK, tarifa
# MAGIC fija. Pickups en aeropuerto: `____ %` de los viajes y `____ %` del revenue; si se cuentan también los viajes que
# MAGIC **terminan** en un aeropuerto, `____ %` del revenue. Por eso Queens pesa en revenue mucho más de lo que su número
# MAGIC de viajes sugiere.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ticket promedio de los viajes que terminan en un aeropuerto: festivos vs. días normales
# MAGIC
# MAGIC Ticket = `AVG(total_amount)`. "Termina en un aeropuerto" = `dropoff_zone_key` → `dim_zone.is_airport`.
# MAGIC Se muestra también cuántos días festivos hay en la muestra (son pocos, el promedio es más volátil) y el detalle por
# MAGIC aeropuerto y por festivo.

# COMMAND ----------

q2 = spark.sql(f"""
SELECT CASE WHEN d.is_holiday THEN 'Festivo' ELSE 'Día normal' END AS tipo_dia,
       COUNT(*)                          AS viajes,
       COUNT(DISTINCT d.date_key)        AS dias,
       ROUND(COUNT(*) / COUNT(DISTINCT d.date_key), 0) AS viajes_por_dia,
       ROUND(AVG(f.total_amount), 2)     AS ticket_promedio,
       ROUND(AVG(f.trip_distance), 2)    AS distancia_promedio
FROM {FACT} f
JOIN {D_ZONE} dz ON f.dropoff_zone_key = dz.zone_key
JOIN {D_DATE} d  ON f.pickup_date_key  = d.date_key
WHERE dz.is_airport
GROUP BY 1 ORDER BY 1
""")
display(q2)

display(spark.sql(f"""
SELECT dz.zone AS aeropuerto_destino,
       ROUND(AVG(CASE WHEN d.is_holiday     THEN f.total_amount END), 2) AS ticket_festivo,
       ROUND(AVG(CASE WHEN NOT d.is_holiday THEN f.total_amount END), 2) AS ticket_normal,
       ROUND(AVG(CASE WHEN d.is_holiday THEN f.total_amount END)
             - AVG(CASE WHEN NOT d.is_holiday THEN f.total_amount END), 2) AS diferencia
FROM {FACT} f
JOIN {D_ZONE} dz ON f.dropoff_zone_key = dz.zone_key
JOIN {D_DATE} d  ON f.pickup_date_key  = d.date_key
WHERE dz.is_airport
GROUP BY dz.zone ORDER BY dz.zone
"""))

display(spark.sql(f"""
SELECT d.full_date, d.day_name, d.holiday_name, COUNT(*) AS viajes,
       ROUND(AVG(f.total_amount), 2) AS ticket_promedio
FROM {FACT} f
JOIN {D_ZONE} dz ON f.dropoff_zone_key = dz.zone_key
JOIN {D_DATE} d  ON f.pickup_date_key  = d.date_key
WHERE dz.is_airport AND d.is_holiday
GROUP BY d.full_date, d.day_name, d.holiday_name ORDER BY d.full_date
"""))

r = {row["tipo_dia"]: row for row in q2.collect()}
if "Festivo" in r and "Día normal" in r:
    diff = r["Festivo"]["ticket_promedio"] - r["Día normal"]["ticket_promedio"]
    print(f"Ticket a aeropuerto: festivo USD {r['Festivo']['ticket_promedio']} vs. normal USD "
          f"{r['Día normal']['ticket_promedio']} (diferencia {diff:+.2f}) sobre {r['Festivo']['dias']} días festivos.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Respuesta 2.** Ticket promedio hacia aeropuertos: festivo `USD ____` vs. día normal `USD ____`
# MAGIC (diferencia `____`), con `____` días festivos en la muestra. Lectura: en festivos hay menos tráfico (viajes más
# MAGIC rápidos y baratos por tiempo detenido) pero también más viajeros de larga distancia; el detalle por aeropuerto
# MAGIC muestra cuál efecto domina. Ojo con la muestra: son ~11 festivos por año frente a ~354 días normales, así que
# MAGIC un solo festivo atípico (Thanksgiving, Navidad) mueve el promedio. Sin `dim_date` con `is_holiday` esta pregunta
# MAGIC exigiría que cada analista mantuviera su propia lista de festivos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Viajes con tarifa JFK (`RatecodeID = 2`): ¿qué porcentaje empieza o termina en JFK Airport?

# COMMAND ----------

q3 = spark.sql(f"""
SELECT CASE WHEN pz.zone = 'JFK Airport' AND dz.zone = 'JFK Airport' THEN '1. Empieza y termina en JFK'
            WHEN pz.zone = 'JFK Airport' THEN '2. Empieza en JFK'
            WHEN dz.zone = 'JFK Airport' THEN '3. Termina en JFK'
            ELSE '4. Ni empieza ni termina en JFK' END AS categoria,
       COUNT(*) AS viajes,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
       ROUND(AVG(f.fare_amount), 2) AS tarifa_promedio
FROM {FACT} f
JOIN {D_RATE} r  ON f.rate_code_key    = r.rate_code_key
JOIN {D_ZONE} pz ON f.pickup_zone_key  = pz.zone_key
JOIN {D_ZONE} dz ON f.dropoff_zone_key = dz.zone_key
WHERE r.rate_code_desc_tlc = 'JFK'
GROUP BY 1 ORDER BY 1
""")
display(q3)

pct_jfk = q3.filter(~F.col("categoria").startswith("4")).agg(F.sum("pct")).first()[0] or 0
print(f"El {pct_jfk:.2f}% de los viajes con tarifa JFK empieza o termina en JFK Airport; "
      f"el {100 - pct_jfk:.2f}% restante no toca JFK.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Qué explica el resto? Detalle de los viajes con tarifa JFK que no tocan JFK

# COMMAND ----------

display(spark.sql(f"""
SELECT pz.borough AS borough_origen, pz.zone AS zona_origen,
       dz.borough AS borough_destino, dz.zone AS zona_destino,
       COUNT(*) AS viajes,
       ROUND(AVG(f.fare_amount), 2)   AS tarifa_promedio,
       ROUND(AVG(f.trip_distance), 2) AS distancia_promedio
FROM {FACT} f
JOIN {D_RATE} r  ON f.rate_code_key    = r.rate_code_key
JOIN {D_ZONE} pz ON f.pickup_zone_key  = pz.zone_key
JOIN {D_ZONE} dz ON f.dropoff_zone_key = dz.zone_key
WHERE r.rate_code_desc_tlc = 'JFK' AND pz.zone <> 'JFK Airport' AND dz.zone <> 'JFK Airport'
GROUP BY ALL ORDER BY viajes DESC LIMIT 25
"""))

display(spark.sql(f"""
SELECT CASE WHEN pz.is_special_zone OR dz.is_special_zone THEN 'Zona 264/265 (ubicación desconocida)'
            WHEN pz.is_airport OR dz.is_airport THEN 'Toca otro aeropuerto (LGA / EWR)'
            WHEN pz.borough = 'Queens' OR dz.borough = 'Queens' THEN 'Origen o destino en Queens (cerca de JFK)'
            ELSE 'Otro (probable código de tarifa mal digitado)' END AS explicacion,
       COUNT(*) AS viajes,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_del_resto
FROM {FACT} f
JOIN {D_RATE} r  ON f.rate_code_key    = r.rate_code_key
JOIN {D_ZONE} pz ON f.pickup_zone_key  = pz.zone_key
JOIN {D_ZONE} dz ON f.dropoff_zone_key = dz.zone_key
WHERE r.rate_code_desc_tlc = 'JFK' AND pz.zone <> 'JFK Airport' AND dz.zone <> 'JFK Airport'
GROUP BY 1 ORDER BY viajes DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Respuesta 3.** El `____ %` de los viajes con `RatecodeID = 2` empieza o termina en JFK Airport. La tarifa JFK es
# MAGIC una **tarifa fija entre JFK y Manhattan** (en ambos sentidos), así que lo esperado es que casi todos toquen JFK.
# MAGIC El resto (`____ %`) se explica, según el detalle anterior, por:
# MAGIC
# MAGIC - **Ubicación desconocida**: origen o destino 264/265, cuando el taxímetro no pudo asignar zona.
# MAGIC - **Zonas vecinas a JFK en Queens**: el pasajero se recoge o se deja justo fuera del polígono del aeropuerto
# MAGIC   (hoteles, estacionamientos, terminales de carga) y el conductor aplica la tarifa del aeropuerto.
# MAGIC - **Error de digitación del conductor**: el `RatecodeID` lo selecciona el conductor en el taxímetro; si queda
# MAGIC   marcado de un viaje anterior, viajes urbanos normales salen con tarifa JFK (se ven tarifas promedio que no
# MAGIC   corresponden a la tarifa fija).
# MAGIC
# MAGIC Es un hallazgo de **calidad de datos** para reportar a la TLC: la regla "tarifa JFK ⇒ toca JFK" no la valida
# MAGIC silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Propina promedio con tarjeta por `day_part` y fin de semana. ¿Cuándo se deja más propina?
# MAGIC
# MAGIC Desde `v_propinas_tarjeta`, re-agregando sobre los boroughs con **sumas** (no promediando promedios):
# MAGIC propina promedio = `SUM(propinas_total) / SUM(viajes_tarjeta)`; tasa = `SUM(propinas_total) / SUM(tarifa_total)`.
# MAGIC El filtro "solo tarjeta" ya lo aplica la vista con `tip_is_recorded`.

# COMMAND ----------

q4 = spark.sql(f"""
SELECT franja_horaria, orden_franja,
       CASE WHEN es_fin_de_semana THEN 'Fin de semana' ELSE 'Entre semana' END AS tipo_dia,
       SUM(viajes_tarjeta) AS viajes_tarjeta,
       ROUND(SUM(propinas_total) / SUM(viajes_tarjeta), 2)        AS propina_promedio_usd,
       ROUND(100 * SUM(propinas_total) / SUM(tarifa_total), 2)    AS tasa_propina_pct
FROM {V_TIP}
GROUP BY franja_horaria, orden_franja, es_fin_de_semana
ORDER BY orden_franja, tipo_dia
""")
display(q4)

best_usd = q4.orderBy(F.desc("propina_promedio_usd")).first()
best_pct = q4.orderBy(F.desc("tasa_propina_pct")).first()
print(f"Mayor propina promedio: {best_usd['franja_horaria']} / {best_usd['tipo_dia']} "
      f"(USD {best_usd['propina_promedio_usd']}). Mayor tasa sobre la tarifa: {best_pct['franja_horaria']} / "
      f"{best_pct['tipo_dia']} ({best_pct['tasa_propina_pct']}%).")

# COMMAND ----------

# MAGIC %md
# MAGIC **Respuesta 4.** La mayor propina promedio en dólares se da en `____` / `____` (`USD ____`), y la mayor tasa
# MAGIC sobre la tarifa en `____` / `____` (`____ %`). Conviene mirar las dos métricas: la propina en dólares sube cuando
# MAGIC los viajes son más largos (madrugada y viajes a aeropuertos), mientras que la tasa refleja la generosidad
# MAGIC relativa del pasajero. Solo se incluyen pagos con tarjeta porque en efectivo y Flex Fare `tip_amount` es 0 aunque
# MAGIC haya habido propina: incluirlos diluiría el promedio y confundiría "no dejó propina" con "no se registró".

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Mezcla de formas de pago por mes: tarjeta, efectivo y Flex Fare

# COMMAND ----------

q5_long = spark.sql(f"""
SELECT d.year_month AS anio_mes, p.payment_group AS grupo_pago,
       COUNT(*) AS viajes,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY d.year_month), 2) AS pct_viajes,
       ROUND(100 * SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER (PARTITION BY d.year_month), 2) AS pct_revenue
FROM {FACT} f
JOIN {D_DATE} d ON f.pickup_date_key  = d.date_key
JOIN {D_PAY}  p ON f.payment_type_key = p.payment_type_key
GROUP BY d.year_month, p.payment_group
ORDER BY anio_mes, grupo_pago
""")
q5 = (q5_long.groupBy("anio_mes").pivot("grupo_pago", ["Tarjeta", "Efectivo", "Flex Fare", "Otros"])
      .agg(F.first("pct_viajes")).na.fill(0).orderBy("anio_mes"))
display(q5)
display(q5_long)

agg5 = q5.agg(*[F.round(F.avg(c), 1).alias(c) for c in ["Tarjeta", "Efectivo", "Flex Fare", "Otros"]]).first()
first_ff = q5.filter(F.col("Flex Fare") > 0).select(F.min("anio_mes")).first()[0]
print(f"Promedio mensual: tarjeta {agg5['Tarjeta']}%, efectivo {agg5['Efectivo']}%, Flex Fare {agg5['Flex Fare']}%, "
      f"otros {agg5['Otros']}%. Primer mes con Flex Fare: {first_ff}.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Respuesta 5.** En promedio mensual: tarjeta `____ %`, efectivo `____ %`, Flex Fare `____ %` y otros (sin
# MAGIC cargo, disputa, anulado, desconocido) `____ %`. La tarjeta domina y el efectivo tiende a perder participación;
# MAGIC Flex Fare aparece desde `____` (es el programa de tarifa acordada por app de la TLC). Implicación para Finanzas:
# MAGIC las propinas registradas solo cubren el `____ %` de los viajes que paga con tarjeta, por eso el total de
# MAGIC propinas del mart es un **piso**, no el total real de propinas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Peso de los recargos sobre `total_amount` por borough de pickup
# MAGIC
# MAGIC Recargos = `congestion_surcharge` + `airport_fee` + `cbd_congestion_fee` (con `NULL` tratado como 0 al sumar,
# MAGIC tal como lo expone la vista). Peso = `SUM(recargos) / SUM(revenue)`: ratio recalculado desde sumas.

# COMMAND ----------

q6 = spark.sql(f"""
SELECT borough_pickup,
       SUM(viajes) AS viajes,
       SUM(revenue) AS revenue,
       ROUND(100 * SUM(recargo_congestion) / SUM(revenue), 2) AS pct_congestion,
       ROUND(100 * SUM(recargo_aeropuerto) / SUM(revenue), 2) AS pct_aeropuerto,
       ROUND(100 * SUM(recargo_cbd)        / SUM(revenue), 2) AS pct_cbd,
       ROUND(100 * SUM(recargos)           / SUM(revenue), 2) AS pct_recargos_total
FROM {V_REV}
GROUP BY borough_pickup
ORDER BY pct_recargos_total DESC
""")
display(q6)

# Solo 2025 (cuando ya existe el cargo CBD)
display(spark.sql(f"""
SELECT borough_pickup,
       ROUND(100 * SUM(recargo_cbd) / SUM(revenue), 2) AS pct_cbd_2025,
       ROUND(100 * SUM(recargos)    / SUM(revenue), 2) AS pct_recargos_2025
FROM {V_REV} WHERE anio >= 2025
GROUP BY borough_pickup ORDER BY pct_recargos_2025 DESC
"""))

top = q6.first()
print(f"Mayor peso de recargos: {top['borough_pickup']} ({top['pct_recargos_total']}% del total_amount).")

# COMMAND ----------

# MAGIC %md
# MAGIC **Respuesta 6.** El borough con mayor peso de recargos es `____` (`____ %` del `total_amount`). Patrón esperado:
# MAGIC
# MAGIC - **Manhattan**: domina `congestion_surcharge` (se cobra a viajes que entran, salen o circulan en Manhattan al sur
# MAGIC   de la calle 96) y, desde enero de 2025, `cbd_congestion_fee` (zona de cobro por congestión del MTA). Como los
# MAGIC   viajes son cortos, un recargo fijo pesa mucho sobre un ticket bajo.
# MAGIC - **Queens**: domina `airport_fee`, que solo se cobra en **pickups** en LaGuardia y JFK.
# MAGIC - **EWR / Staten Island / Bronx**: peso bajo; viajes largos y fuera de las zonas de cobro.
# MAGIC
# MAGIC Que `cbd_congestion_fee` sea `NULL` antes de 2025 no altera la suma; si el análisis cubre 2023–2025 completo, el
# MAGIC peso del CBD queda diluido: por eso se muestra también solo 2025.