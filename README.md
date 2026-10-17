# Data mart dimensional — NYC Taxi (Finanzas TLC)

Ejercicio de data mart en modelo estrella sobre la capa Gold, a partir de silver.
Curso de Big Data — Maestría en Ciencia de Datos, UPB.
Autor: Sebastián Muñoz Palacio

## Contenido

| Carpeta / archivo | Qué es |
|---|---|
| `config.py` | Config del pipeline con las constantes `T_DM_*` del mart |
| `notebooks/` | Código fuente de los notebooks (Databricks) |
| `notebooks_html/` | Los mismos notebooks con sus resultados (abrir en el navegador) |
| `evidencias/` | Capturas: llaves, permisos, prueba del compañero, plan y linaje |

## Notebooks (orden de ejecución)

1. `01_dm_dimensions` — Parte 0 (diseño) y Parte 1 (6 dimensiones)
2. `02_dm_fact_trips` — Parte 2 (tabla de hechos y conciliación)
3. `03_dm_constraints_views_grants` — Parte 3 (PK/FK, vistas y permisos)
4. `03b_dm_partner_test` — Prueba de permisos que corre el compañero
5. `04_dm_business_questions` — Parte 4 (6 preguntas de negocio)
6. `05_dm_performance_orchestration` — Partes 5 y 6 (rendimiento y DAG)

## Modelo

- **Grano:** un viaje por fila (`gold_taxi_dm_fact_trips`)
- **Dimensiones:** `dim_date`, `dim_time`, `dim_zone` (role-playing: pickup y dropoff),
  `dim_payment_type`, `dim_rate_code`, `dim_vendor`
- **Vistas para Finanzas:** `v_revenue_mensual`, `v_propinas_tarjeta`
- **Diseño físico:** liquid clustering por `(pickup_date_key, pickup_zone_key)`
