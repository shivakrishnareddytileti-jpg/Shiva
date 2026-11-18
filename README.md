# Smart Meter Energy — SDP Transformation Pipeline

`SDP_transformation.py` defines a **Lakeflow Spark Declarative Pipeline (SDP)** that ingests raw smart-meter JSON readings and processes them through a medallion architecture (Bronze → Silver → Gold) in Unity Catalog.

## Architecture

| Layer | Purpose |
| --- | --- |
| Bronze | Incrementally ingest raw JSON readings from landing volumes via Auto Loader (`cloudFiles`), adding an `ingested_at` timestamp |
| Silver | Enforce data-quality expectations on Bronze readings (drop invalid rows) |
| Gold | Aggregate Silver readings into daily per-meter consumption summaries (materialized views) |

## Source Data

Raw readings are read from Unity Catalog volumes:

* `/Volumes/energy_dev/smart_meter/landing/readings/` — primary landing path (`*.json`)
* `/Volumes/energy_dev/smart_meter/landing/new_read/` — secondary landing path

Each JSON record has the schema:

| Column | Type |
| --- | --- |
| `reading_id` | STRING |
| `meter_id` | STRING |
| `reading_time` | TIMESTAMP |
| `interval_kwh` | DOUBLE |

## Tables Defined

### Bronze

* `bronze_meter_readings` — streaming table from `landing/readings/` with `ingested_at` audit column
* `bronze_meter_reading_new` — streaming table from `landing/new_read/` (JSON, `pathGlobFilter *.json`), also with `ingested_at`

### Silver

* `silver_meter_readings` — validated copy of `bronze_meter_readings`
* `silver_meter_readings_new` — validated copy of `bronze_meter_reading_new`

Both apply these `expect_or_drop` data-quality rules (rows failing any rule are dropped):

* `valid_reading_id` — `reading_id IS NOT NULL`
* `valid_meter_id` — `meter_id IS NOT NULL`
* `valid_time` — `reading_time IS NOT NULL`
* `valid_consumption` — `interval_kwh IS NOT NULL AND interval_kwh >= 0`

### Gold

* `gold_daily_consumption` — daily consumption per meter from `silver_meter_readings`
* `gold_daily_consumption_new` — daily consumption per meter from `silver_meter_readings_new`

Both are materialized views grouped by `meter_id` and `reading_date` (`reading_time` truncated to date) producing:

* `daily_kwh` — total kWh consumed that day, rounded to 2 decimals
* `reading_count` — number of readings contributing to that day

## How to Run

1. Add this file as a source notebook to a Lakeflow Spark Declarative Pipeline.
2. Configure the pipeline to publish tables to the `energy_dev.smart_meter` schema (or your target Unity Catalog schema).
3. Land JSON files in the source volumes listed above.
4. Start a pipeline update — Bronze tables stream new files incrementally, Silver tables apply expectations, and Gold materialized views refresh with daily aggregates.

## Dependencies

* Databricks workspace with Unity Catalog
* Volumes `energy_dev.smart_meter.landing` containing raw JSON readings
* Lakeflow Spark Declarative Pipelines (PySpark `pyspark.pipelines` API)