# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Data mart Finanzas TLC — 03b · Prueba de permisos (lo corre el COMPAÑERO)
# MAGIC
# MAGIC Este notebook lo ejecuta tu compañero en **su** sesión, con **tu** schema en el widget `schema_duenio`.
# MAGIC Demuestra que puede leer las vistas y que **no** puede leer `fact_trips` ni `silver_yellow_trips_enriched`.
# MAGIC No usa `%run ../00-setup/config` a propósito: el config del compañero apunta a su propio schema.

# COMMAND ----------

dbutils.widgets.text("catalogo", "51810_databricks", "Catálogo del curso")
dbutils.widgets.text("schema_duenio", "", "Schema del dueño del mart (nombre_apellido)")
CAT = dbutils.widgets.get("catalogo").strip()
OWNER = dbutils.widgets.get("schema_duenio").strip()
assert OWNER, "Escribe en el widget el schema de tu compañero (dueño del data mart)"

p = f"`{CAT}`.`{OWNER}`"
DEBE_LEER = [f"{p}.gold_taxi_dm_v_revenue_mensual", f"{p}.gold_taxi_dm_v_propinas_tarjeta"]
NO_DEBE_LEER = [f"{p}.gold_taxi_dm_fact_trips", f"{p}.silver_yellow_trips_enriched"]

# COMMAND ----------

print(f"Usuario actual: {spark.sql('SELECT current_user()').first()[0]}\n")
resultados = []
for t in DEBE_LEER + NO_DEBE_LEER:
    try:
        n = spark.sql(f"SELECT COUNT(*) FROM {t}").first()[0]
        resultados.append((t, "LEE", f"{n:,} filas"))
        print(f"✓ {t}: {n:,} filas")
    except Exception as e:
        msg = str(e).split("\n")[0][:300]
        resultados.append((t, "ERROR", msg))
        print(f"✗ {t}: {msg}")

display(spark.createDataFrame(resultados, "objeto STRING, resultado STRING, detalle STRING"))

ok_vistas = all(r[1] == "LEE" for r in resultados[:len(DEBE_LEER)])
ok_tablas = all(r[1] == "ERROR" for r in resultados[len(DEBE_LEER):])
print("\nPrueba de permisos:", "SUPERADA ✓" if ok_vistas and ok_tablas else "NO SUPERADA ✗")