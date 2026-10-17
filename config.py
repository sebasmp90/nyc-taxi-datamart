# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Configuración centralizada del pipeline NYC Taxi
# MAGIC
# MAGIC Este notebook se importa con `%run ../00-setup/config`.
# MAGIC Centraliza paths, schemas y parámetros del pipeline.
# MAGIC
# MAGIC ## Arquitectura real del curso
# MAGIC
# MAGIC - **Catálogo compartido**: `20080_databricks`, ya creado por el profesor.
# MAGIC   El estudiante NO crea catálogos ni schemas: solo necesita `USE CATALOG`
# MAGIC   sobre el catálogo y ser dueño de su schema, que también crea el profesor.
# MAGIC   El nombre del catálogo cambia entre iteraciones
# MAGIC   del curso: confírmalo con tu docente antes de correr nada.
# MAGIC   Como empieza por dígito, en SQL siempre va entre backticks.
# MAGIC - **Un schema por estudiante**: `<CATALOG>.<SCHEMA>`, donde `SCHEMA` es tu
# MAGIC   `nombre_apellido` en snake_case (por ejemplo `camilo_soto`). Es lo ÚNICO que
# MAGIC   debes editar en este notebook.
# MAGIC - **Landing de solo lectura**: Volumes UC del schema `default` del catálogo
# MAGIC   compartido — `/Volumes/<CATALOG>/default/nytaxi_landing` (parquets mensuales
# MAGIC   de NYC TLC) y `/Volumes/<CATALOG>/default/nytaxi_zones` (lookup de zonas).
# MAGIC   Los puebla el profesor con `_admin_download_landing.py`.
# MAGIC - **Capas medallion como tablas administradas de Unity Catalog**: bronze,
# MAGIC   silver, gold y streaming se escriben con `saveAsTable` / `toTable` en
# MAGIC   `<CATALOG>.<SCHEMA>`. UC gestiona el almacenamiento, así que las tablas se
# MAGIC   consultan por nombre desde SQL, aparecen en Catalog Explorer con linaje y
# MAGIC   admiten `GRANT` por tabla. Las capas se distinguen por prefijo en el nombre
# MAGIC   (`bronze_`, `silver_`, `gold_`, `stream_`).
# MAGIC - **Volume `datos`** (el que ya tiene cada estudiante en su schema): este
# MAGIC   módulo solo lo usa para lo que NO es tabla — checkpoints de Structured
# MAGIC   Streaming y el schema location de Auto Loader, bajo `_checkpoints/ny_taxi`.
# MAGIC - **Modelo de ML en tu mismo schema**: `<CATALOG>.<SCHEMA>.trip_duration_gbt`.
# MAGIC   No hay schema aparte para ML, porque el estudiante no tiene `CREATE SCHEMA`.

# COMMAND ----------

# =============================================================================
# Unity Catalog — Catálogo, Schema y Volume
# =============================================================================
# ── Bloque editable por el estudiante ───────────────────────────────────────
# SOLO debes cambiar SCHEMA por tu nombre_apellido en snake_case.
# CATALOG lo crea el profesor: no lo inventes, confirma con tu docente el del
# semestre. VOLUME no se cambia: `datos` es el Volume que ya existe en el
# schema de cada estudiante.
CATALOG =  "51810_databricks" # lo crea el profesor; confírmalo con tu docente
SCHEMA =   "sebastianc_munoz"     # ← CAMBIAR: tu nombre_apellido en snake_case
VOLUME = "datos"              # NO cambiar

# =============================================================================
# Volume del estudiante: SOLO archivos que no son tablas (checkpoints)
# =============================================================================
# En Unity Catalog la ruta de un Volume es /Volumes/<catalogo>/<schema>/<volume>/...
# Las capas medallion NO viven aquí: son tablas administradas (ver más abajo).
DATALAKE_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# =============================================================================
# Landing compartido de solo lectura (Volumes UC, preparados por el profesor)
# =============================================================================
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/default/nytaxi_landing"
ZONES_VOLUME_PATH = f"/Volumes/{CATALOG}/default/nytaxi_zones"
# Alias usados por los notebooks downstream
LANDING_TRIPS_PATH = LANDING_VOLUME_PATH
LANDING_ZONES_PATH = ZONES_VOLUME_PATH

# =============================================================================
# Tablas administradas de Unity Catalog (catálogo.schema.tabla)
# =============================================================================
# Todas las capas viven en tu schema. El catálogo empieza por dígito, así que
# va entre backticks; estos nombres sirven tal cual en spark.table(),
# saveAsTable(), DeltaTable.forName() y dentro de spark.sql().
#
# El módulo 04 escribe en este mismo schema (bronze_travel_times, ...), por eso
# los nombres de este módulo llevan `yellow_trips` / `taxi_` para no chocar.
TABLE_PREFIX = f"`{CATALOG}`.`{SCHEMA}`"

# ── Bronze — datos crudos tal cual llegan ──────────────────────────────────
T_BRONZE_TRIPS = f"{TABLE_PREFIX}.bronze_yellow_trips"
T_BRONZE_ZONES = f"{TABLE_PREFIX}.bronze_taxi_zones"

# ── Silver — datos limpios, enriquecidos y métricas de calidad ─────────────
T_SILVER_TRIPS = f"{TABLE_PREFIX}.silver_yellow_trips"
T_SILVER_REJECTED = f"{TABLE_PREFIX}.silver_yellow_trips_rejected"
T_SILVER_TRIPS_ENRICHED = f"{TABLE_PREFIX}.silver_yellow_trips_enriched"
T_SILVER_QUALITY_METRICS = f"{TABLE_PREFIX}.silver_yellow_trips_quality"

# ── Gold — KPIs de negocio y features de ML ────────────────────────────────
T_GOLD_REVENUE_BY_ZONE = f"{TABLE_PREFIX}.gold_taxi_revenue_by_zone"
T_GOLD_DAILY_METRICS = f"{TABLE_PREFIX}.gold_taxi_daily_metrics"
T_GOLD_HOURLY_DEMAND = f"{TABLE_PREFIX}.gold_taxi_hourly_demand"
T_GOLD_ML_FEATURES = f"{TABLE_PREFIX}.gold_taxi_ml_features"

# ── ML — predicciones batch ────────────────────────────────────────────────
T_ML_PREDICTIONS = f"{TABLE_PREFIX}.gold_taxi_ml_predictions"

# ── Streaming — tablas de Structured Streaming ─────────────────────────────
T_STREAM_BRONZE = f"{TABLE_PREFIX}.stream_bronze_yellow_trips"
T_STREAM_SILVER = f"{TABLE_PREFIX}.stream_silver_yellow_trips"

# Checkpoints y schema location de Auto Loader: son archivos, no tablas,
# así que sí van en el Volume. Subcarpeta propia para no mezclarse con lo que
# otros módulos guardan en `datos` (landing/, bronze/, silver/).
CHECKPOINT_BASE = f"{DATALAKE_BASE}/_checkpoints/ny_taxi"

# Directorio temporal de MLflow para guardar/cargar modelos Spark MLlib. En
# compute serverless o de acceso estándar, mlflow.spark exige un Volume de Unity
# Catalog: sin él falla con "UC volume path must be provided to save, log or
# load SparkML models". En compute dedicado no hace falta, pero no estorba.
MLFLOW_DFS_TMPDIR = f"{DATALAKE_BASE}/_mlflow_tmp/ny_taxi"

# =============================================================================
# Modelo registrado en Unity Catalog
# =============================================================================
# Va en tu mismo schema: eres su dueño, así que tienes CREATE MODEL sin grants
# extra. (Los estudiantes no tienen CREATE SCHEMA sobre el catálogo, así que un
# schema aparte para ML tendría que crearlo el profesor para cada uno.)
MODEL_NAME = f"{CATALOG}.{SCHEMA}.trip_duration_gbt"

# Iniciales del estudiante (derivadas del schema)
USER_INITIALS = SCHEMA.upper() if SCHEMA else "STU"

# COMMAND ----------

# =============================================================================
# Esquema canónico del landing (Yellow Taxi TLC)
# =============================================================================
# Inconsistencias detectadas en los Parquet mensuales de la TLC:
#
#   Tipos que cambian entre enero 2023 y el resto:
#     VendorID        → Ene 2023: BIGINT,  Feb–Dic 2023 y 2024: INT
#     PULocationID    → Ene 2023: BIGINT,  Feb–Dic 2023 y 2024: INT
#     DOLocationID    → Ene 2023: BIGINT,  Feb–Dic 2023 y 2024: INT
#     RatecodeID      → Ene 2023: DOUBLE,  Feb–Dic 2023 y 2024: BIGINT
#     passenger_count → Ene 2023: DOUBLE,  Feb–Dic 2023 y 2024: BIGINT
#
#   Nombre de columna que cambia:
#     airport_fee     → solo en Ene 2023 (minúscula)
#     Airport_fee     → Feb–Dic 2023, 2024, 2025 (mayúscula inicial)
#
#   Columna nueva en 2025:
#     cbd_congestion_fee → cargo por Congestion Relief Zone del MTA (desde 5 ene 2025)
#
# Estrategia:
#   Definimos un esquema nativo por variante (v1 = Ene 2023, v2 = resto 2023/2024,
#   v3 = 2025+) para leer sin errores, y luego casteamos al esquema unificado.
# =============================================================================
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DoubleType, StringType, TimestampNTZType,
)

# ── Esquema unificado: el esquema final que tendrán TODOS los DataFrames ─────
# Incluye cbd_congestion_fee (nueva desde 2025); para años anteriores será null.
UNIFIED_SCHEMA = StructType([
    StructField("VendorID",              LongType()),
    StructField("tpep_pickup_datetime",  TimestampNTZType()),
    StructField("tpep_dropoff_datetime", TimestampNTZType()),
    StructField("passenger_count",       DoubleType()),
    StructField("trip_distance",         DoubleType()),
    StructField("RatecodeID",            DoubleType()),
    StructField("store_and_fwd_flag",    StringType()),
    StructField("PULocationID",          LongType()),
    StructField("DOLocationID",          LongType()),
    StructField("payment_type",          LongType()),
    StructField("fare_amount",           DoubleType()),
    StructField("extra",                 DoubleType()),
    StructField("mta_tax",              DoubleType()),
    StructField("tip_amount",            DoubleType()),
    StructField("tolls_amount",          DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount",          DoubleType()),
    StructField("congestion_surcharge",  DoubleType()),
    StructField("airport_fee",           DoubleType()),
    StructField("cbd_congestion_fee",    DoubleType()),
])

# Para compatibilidad con código existente que referencie LANDING_SCHEMA
LANDING_SCHEMA = UNIFIED_SCHEMA

# ── Esquemas nativos por variante ────────────────────────────────────────────
# v1: Enero 2023 — tipos originales del TLC para ese mes
_SCHEMA_V1 = StructType([
    StructField("VendorID",              LongType()),
    StructField("tpep_pickup_datetime",  TimestampNTZType()),
    StructField("tpep_dropoff_datetime", TimestampNTZType()),
    StructField("passenger_count",       DoubleType()),
    StructField("trip_distance",         DoubleType()),
    StructField("RatecodeID",            DoubleType()),
    StructField("store_and_fwd_flag",    StringType()),
    StructField("PULocationID",          LongType()),
    StructField("DOLocationID",          LongType()),
    StructField("payment_type",          LongType()),
    StructField("fare_amount",           DoubleType()),
    StructField("extra",                 DoubleType()),
    StructField("mta_tax",              DoubleType()),
    StructField("tip_amount",            DoubleType()),
    StructField("tolls_amount",          DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount",          DoubleType()),
    StructField("congestion_surcharge",  DoubleType()),
    StructField("airport_fee",           DoubleType()),       # minúscula
])

# v2: Feb–Dic 2023, todo 2024 — tipos cambiados por la TLC
_SCHEMA_V2 = StructType([
    StructField("VendorID",              IntegerType()),
    StructField("tpep_pickup_datetime",  TimestampNTZType()),
    StructField("tpep_dropoff_datetime", TimestampNTZType()),
    StructField("passenger_count",       LongType()),
    StructField("trip_distance",         DoubleType()),
    StructField("RatecodeID",            LongType()),
    StructField("store_and_fwd_flag",    StringType()),
    StructField("PULocationID",          IntegerType()),
    StructField("DOLocationID",          IntegerType()),
    StructField("payment_type",          LongType()),
    StructField("fare_amount",           DoubleType()),
    StructField("extra",                 DoubleType()),
    StructField("mta_tax",              DoubleType()),
    StructField("tip_amount",            DoubleType()),
    StructField("tolls_amount",          DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount",          DoubleType()),
    StructField("congestion_surcharge",  DoubleType()),
    StructField("Airport_fee",           DoubleType()),       # Mayúscula
])

# v3: 2025 en adelante — nueva columna cbd_congestion_fee
_SCHEMA_V3 = StructType([
    StructField("VendorID",              IntegerType()),
    StructField("tpep_pickup_datetime",  TimestampNTZType()),
    StructField("tpep_dropoff_datetime", TimestampNTZType()),
    StructField("passenger_count",       LongType()),
    StructField("trip_distance",         DoubleType()),
    StructField("RatecodeID",            LongType()),
    StructField("store_and_fwd_flag",    StringType()),
    StructField("PULocationID",          IntegerType()),
    StructField("DOLocationID",          IntegerType()),
    StructField("payment_type",          LongType()),
    StructField("fare_amount",           DoubleType()),
    StructField("extra",                 DoubleType()),
    StructField("mta_tax",              DoubleType()),
    StructField("tip_amount",            DoubleType()),
    StructField("tolls_amount",          DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount",          DoubleType()),
    StructField("congestion_surcharge",  DoubleType()),
    StructField("Airport_fee",           DoubleType()),       # Mayúscula
    StructField("cbd_congestion_fee",    DoubleType()),
])

# ── Mapeo: qué variante de esquema usa cada mes ─────────────────────────────
# v1: Enero 2023 (BIGINT + minúscula airport_fee)
# v2: Feb–Dic 2023, todo 2024 (INT + mayúscula Airport_fee)
# v3: 2025+ (igual que v2 + cbd_congestion_fee)
_MONTH_SCHEMA_MAP = {
    # 2023
    "2023-01": "v1",
    "2023-02": "v2", "2023-03": "v2", "2023-04": "v2",
    "2023-05": "v2", "2023-06": "v2", "2023-07": "v2",
    "2023-08": "v2", "2023-09": "v2", "2023-10": "v2",
    "2023-11": "v2", "2023-12": "v2",
    # 2024
    "2024-01": "v2", "2024-02": "v2", "2024-03": "v2",
    "2024-04": "v2", "2024-05": "v2", "2024-06": "v2",
    "2024-07": "v2", "2024-08": "v2", "2024-09": "v2",
    "2024-10": "v2", "2024-11": "v2", "2024-12": "v2",
    # 2025
    "2025-01": "v3", "2025-02": "v3", "2025-03": "v3",
    "2025-04": "v3", "2025-05": "v3", "2025-06": "v3",
    "2025-07": "v3", "2025-08": "v3", "2025-09": "v3",
    "2025-10": "v3", "2025-11": "v3", "2025-12": "v3",
}

_SCHEMA_VARIANTS = {
    "v1": _SCHEMA_V1,
    "v2": _SCHEMA_V2,
    "v3": _SCHEMA_V3,
}

# Columnas que necesitan casteo de v2/v3 → unificado
_V2_CASTS = {
    "VendorID":        "long",
    "PULocationID":    "long",
    "DOLocationID":    "long",
    "passenger_count": "double",
    "RatecodeID":      "double",
}

# Columnas que necesitan renombrar por variante (v2 y v3)
_V2_RENAMES = {
    "Airport_fee": "airport_fee",
}

# COMMAND ----------

def _detect_variant(filename):
    """Extrae YYYY-MM del nombre del archivo y devuelve la variante de esquema.

    Lógica:
      1. Si el mes está en _MONTH_SCHEMA_MAP, usa esa variante explícita.
      2. Si el año es >= 2025, usa v3 (tiene cbd_congestion_fee).
      3. En cualquier otro caso, usa v2.
    """
    import re
    match = re.search(r"(\d{4})-(\d{2})", filename)
    if not match:
        return "v2"

    month_key = match.group(0)  # "YYYY-MM"
    if month_key in _MONTH_SCHEMA_MAP:
        return _MONTH_SCHEMA_MAP[month_key]

    year = int(match.group(1))
    if year >= 2025:
        return "v3"
    return "v2"


def _read_and_unify(spark, file_path, variant):
    """Lee un Parquet con su esquema nativo y lo transforma al esquema unificado."""
    from pyspark.sql import functions as F

    schema = _SCHEMA_VARIANTS[variant]
    df = (
        spark.read.schema(schema).parquet(file_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    # Aplicar renombramientos (v2 y v3 tienen Airport_fee → airport_fee)
    renames = _V2_RENAMES if variant in ("v2", "v3") else {}
    for old_name, new_name in renames.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    # Aplicar casteos al esquema unificado (v2 y v3 necesitan los mismos)
    casts = _V2_CASTS if variant in ("v2", "v3") else {}
    for col_name, target_type in casts.items():
        df = df.withColumn(col_name, F.col(col_name).cast(target_type))

    # Agregar cbd_congestion_fee como null para variantes que no la tienen
    if variant in ("v1", "v2"):
        df = df.withColumn("cbd_congestion_fee", F.lit(None).cast("double"))

    return df


def read_yellow_trips(spark, path=None):
    """Lee todos los Parquet del landing, normalizando esquemas por mes.

    Cada archivo se lee con su esquema nativo (v1, v2 o v3 según el mes) y se
    transforma al UNIFIED_SCHEMA. Luego se unen con unionByName.
    """
    from functools import reduce

    if path is None:
        path = LANDING_VOLUME_PATH

    files = [f for f in dbutils.fs.ls(path) if f.name.endswith(".parquet")]

    if not files:
        raise FileNotFoundError(f"No se encontraron archivos .parquet en {path}")

    dfs = []
    for f in files:
        variant = _detect_variant(f.name)
        df = _read_and_unify(spark, f.path, variant)
        dfs.append(df)
        print(f"  ✓ {f.name:45s} → esquema {variant}")

    return reduce(lambda a, b: a.unionByName(b), dfs)


def read_yellow_trips_month(spark, year: int, month: int, path=None):
    """Lee un único Parquet mensual del landing y lo devuelve con esquema unificado.

    Diseñado para la capa bronze donde se hace append mes a mes.

    Args:
        spark: SparkSession
        year:  Año del archivo (e.g. 2024)
        month: Mes del archivo (1–12)
        path:  Directorio base de los parquets. Default: LANDING_VOLUME_PATH

    Returns:
        DataFrame con UNIFIED_SCHEMA + columna _source_file

    Raises:
        FileNotFoundError: si el archivo no existe en el landing

    Ejemplo de uso en bronze:
        df = read_yellow_trips_month(spark, 2024, 3)
        df.write.format("delta").mode("append").saveAsTable(T_BRONZE_TRIPS)
    """
    if path is None:
        path = LANDING_VOLUME_PATH

    month_str = f"{year}-{month:02d}"
    filename = f"yellow_tripdata_{month_str}.parquet"
    file_path = f"{path}/{filename}"

    # Verificar que el archivo existe
    import os
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"No se encontró el archivo {filename} en {path}. "
            f"¿Ya se descargó al landing?"
        )

    variant = _detect_variant(filename)
    df = _read_and_unify(spark, file_path, variant)
    print(f"  ✓ {filename} → esquema {variant} ({df.count():,.0f} registros)")
    return df

# COMMAND ----------

print(f"Catalog:          {CATALOG}")
print(f"Schema:           {SCHEMA}")
print(f"Volume:           {VOLUME}")
print(f"Landing trips:    {LANDING_TRIPS_PATH}")
print(f"Landing zones:    {LANDING_ZONES_PATH}")
print(f"Bronze trips:     {T_BRONZE_TRIPS}")
print(f"Bronze zones:     {T_BRONZE_ZONES}")
print(f"Silver trips:     {T_SILVER_TRIPS}")
print(f"Silver enriched:  {T_SILVER_TRIPS_ENRICHED}")
print(f"Gold ml_features: {T_GOLD_ML_FEATURES}")
print(f"Checkpoints:      {CHECKPOINT_BASE}")
print(f"Modelo UC:        {MODEL_NAME}")

# COMMAND ----------

# ── Gold — Data mart dimensional (modelo estrella para Finanzas TLC) ──
T_DM_DIM_DATE         = f"{TABLE_PREFIX}.gold_taxi_dm_dim_date"
T_DM_DIM_TIME         = f"{TABLE_PREFIX}.gold_taxi_dm_dim_time"
T_DM_DIM_ZONE         = f"{TABLE_PREFIX}.gold_taxi_dm_dim_zone"
T_DM_DIM_PAYMENT_TYPE = f"{TABLE_PREFIX}.gold_taxi_dm_dim_payment_type"
T_DM_DIM_RATE_CODE    = f"{TABLE_PREFIX}.gold_taxi_dm_dim_rate_code"
T_DM_DIM_VENDOR       = f"{TABLE_PREFIX}.gold_taxi_dm_dim_vendor"
T_DM_FACT_TRIPS       = f"{TABLE_PREFIX}.gold_taxi_dm_fact_trips"

# Vistas de consumo (lo único que ve Finanzas)
T_DM_V_REVENUE_MENSUAL  = f"{TABLE_PREFIX}.gold_taxi_dm_v_revenue_mensual"
T_DM_V_PROPINAS_TARJETA = f"{TABLE_PREFIX}.gold_taxi_dm_v_propinas_tarjeta"

print(f"Data mart fact:   {T_DM_FACT_TRIPS}")