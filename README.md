# Transportation Databricks SDP (Spark Declarative Pipelines)

An end-to-end **medallion (bronze -> silver -> gold)** data pipeline built with **Databricks Lakeflow Spark Declarative Pipelines (SDP)** and **PySpark**, modeling ride-trip data for *Goodcabs*, a cab service operating across ten tier-2 Indian cities.

The pipeline ingests raw city and trip data from cloud storage, cleans and validates it, and serves analytics-ready fact views, including per-city breakouts for downstream reporting.

## Architecture

![Architecture](3.%20architecture/architecture.png)

Data flows through three layers following the medallion pattern:

| Layer | Purpose | Tech |
|-------|---------|------|
| **Bronze** | Raw ingestion from cloud storage, minimal transformation, lineage metadata | PySpark, Delta Live Tables, Auto Loader (streaming CSV) |
| **Silver** | Cleaning, standardization, data-quality expectations, SCD Type 1 CDC upserts, calendar dimension | PySpark, DLT expectations, `create_auto_cdc_flow` |
| **Gold** | Analytics-ready fact views (star schema) and per-city fact views | SQL views |

## Project Structure

```
.
├── project_setup.py            # Creates catalog + bronze/silver/gold schemas
├── 1. data/                    # Sample source data
│   ├── city/                   # City dimension (city.csv)
│   └── trips/
│       ├── Full Load/          # Initial batch of daily trip exports
│       └── Incremental Load/   # Incremental daily exports (Auto Loader)
├── 2. code/
│   ├── bronze/                 # Raw ingestion (city.py, trips.py)
│   ├── silver/                 # Cleaning, validation, CDC (city, trips, calendar)
│   └── gold/                   # Fact views (trips_gold.sql + per-city views)
└── 3. architecture/            # Architecture diagram
```

## Pipeline Details

### Bronze

- **`city.py`**: Batch-reads the city dimension CSV into a materialized view, adding `file_name` and `ingest_datetime` lineage columns.
- **`trips.py`**: Streams daily trip CSV exports with **Auto Loader** (`cloudFiles`), using rescue-mode schema evolution and renaming the problematic `distance_travelled(km)` header.

### Silver

- **`city.py`**: Standardizes the city dimension and carries ingest timestamps forward.
- **`calendar.py`**: Generates a full calendar dimension (year/month/quarter, weekday/weekend flags, ISO week, and Indian national holidays) over a configurable date range.
- **`trips.py`**: Applies data-quality **expectations** (valid date, driver/passenger rating bounds), renames columns to business-friendly names, and performs an **SCD Type 1 CDC upsert** into a streaming table keyed on `trip_id`.

### Gold

- **`trips_gold.sql`**: `fact_trips` view joining trips to the city and calendar dimensions (star schema).
- **`trips_<city>.sql`**: Ten per-city fact views (Chandigarh, Coimbatore, Indore, Jaipur, Kochi, Lucknow, Mysore, Surat, Vadodara, Visakhapatnam) for city-level reporting.

## Tech Stack

- **Databricks** (Unity Catalog, Delta Live Tables / Lakeflow Declarative Pipelines)
- **PySpark** (`pyspark.pipelines`, Structured Streaming, Auto Loader)
- **Delta Lake** (Change Data Feed, auto-optimize / auto-compact)
- **SQL** (gold-layer analytics views)

## Getting Started

1. Run `project_setup.py` in Databricks to create the `transportation` catalog and the `bronze` / `silver` / `gold` schemas.
2. Upload the source data (or point `SOURCE_PATH` at your own cloud storage; the bronze scripts reference an S3 path).
3. Build the DLT pipeline from the `2. code/bronze` and `2. code/silver` scripts, passing `start_date` / `end_date` config for the calendar.
4. Create the gold-layer views from the SQL in `2. code/gold`.

## Notes

Column names in the source data are standardized in the silver layer (e.g. `trip_id → id`, `fare_amount → sales_amt`, `distance_travelled(km) → distance_kms`). The bronze scripts read from an S3 bucket in production; the CSVs under `1. data/` are provided as sample source data.
