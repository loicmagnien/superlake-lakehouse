# Lakehouse Data Platform

## Overview

This repository is an **example project** designed to show how to build a project leveraging the [superlake](https://github.com/loicmagnien/superlake) library. It implements a modular Lakehouse architecture for ingesting, modeling, and orchestrating data pipelines using PySpark and Delta Lake. It supports multi-source data ingestion (ERP, CRM, open data), transformation into bronze/silver/gold tables, and dimensional modeling for analytics and reporting.

## Repository Structure

```
.
├── requirements.txt         # Python dependencies
├── data/                   # Data storage (warehouse, external tables, etc.)
├── lakehouse/              # Main source code
│   ├── ingestion/          # Data ingestion pipelines (ERP, CRM, Open Data)
│   ├── modelisation/       # Data modeling (facts, dimensions)
│   ├── orchestration/      # Pipeline orchestration logic
│   └── utils/              # Utility scripts (e.g., Opendatasoft loader)
└── ...
```

### Key Submodules
- **ingestion/**: Scripts to extract and load data from various sources into the lakehouse (ERP, CRM, open data APIs).
- **modelisation/**: Contains `facts/` and `dimensions/` for star schema modeling.
- **orchestration/**: Pipeline orchestration and main entry point.
- **utils/**: Helper utilities (e.g., API loaders).

## Data Flow & Architecture

1. **Ingestion**: Data is ingested from multiple sources:
   - **ERP**: Sales transactions, etc.
   - **CRM**: Contacts, customer info.
   - **Open Data**: Bike station status/info (e.g., Velib, Velov).

2. **Bronze/Silver/Gold Tables**:
   - **Bronze**: Raw ingested data, lightly cleaned.
   - **Silver**: Transformed, typed, and enriched data.
   - **Gold (DWH)**: Fact and dimension tables for analytics.

3. **Modelisation**:
   - **Fact Tables**: E.g., `fact_sales`, `fact_bike_status` (sales, bike station status).
   - **Dimension Tables**: E.g., `dim_customer`, `dim_product`, `dim_bike_station`, `dim_date`, etc.

4. **Orchestration**:
   - Managed by `super_orchestrator.py`, which configures and runs the pipelines.

## Key Components

### Ingestion Pipelines
- **ERP**: `erp_sales_transactions.py` — Loads sales transactions (generated).
- **CRM**: `crm_contacts.py` — Loads contact/customer data (generated).
- **Open Data**: `velib_station_status.py`, `velov_station_status.py`, etc. — Loads bike station data from public APIs.

### Fact Tables
- **fact_sales**: Sales transactions, with surrogate keys for dimensions.
- **fact_bike_status**: Bike station status (availability, etc.) from Velib/Velov.

### Dimension Tables
- **dim_customer**: Customer master data (from ERP and CRM).
- **dim_product**: Product master data.
- **dim_store**: Store locations.
- **dim_promo**: Promotions.
- **dim_bike_station**: Bike station metadata (from open data APIs).
- **dim_date**: Calendar/date dimension.

### Utilities
- **opendatasoft.py**: Loads datasets from Opendatasoft APIs into Spark DataFrames, with pagination and retry logic.

### Orchestration
- **super_orchestrator.py**: Main entry point. Sets up Spark, logging, tracing, and runs the orchestrator to execute pipelines.
- **Orchestration Features** (via `SuperOrchestrator.orchestrate`):
  - **Dependency graph analysis**: Automatically discovers dependencies between pipeline files.
  - **Cycle detection**: Detects and logs cyclic dependencies in the pipeline graph.
  - **Group-based orchestration**: Pipelines are grouped by dependency level and processed in order (roots to leaves or vice versa).
  - **Thread-safe status tracking**: Tracks the status (success, failed, skipped) of each pipeline.
  - **Contextual logging**: Logs show which pipeline or orchestrator step is being executed.
  - **Partial graph execution**: Orchestrate only a subset of the full pipeline graph by specifying targets and direction.
  - **Cascading skips**: If a pipeline is skipped due to all upstreams failing/skipped, its downstreams will also be skipped in cascade.

## Installation & Requirements

- **Python**: 3.8+
- **Dependencies**: See `requirements.txt`:
  - `pyspark==3.5.0`
  - `delta-spark==3.1.0`
  - `requests`

Install dependencies:
```bash
pip install -r requirements.txt
```

## How to Run

The main entry point is `lakehouse/orchestration/super_orchestrator.py`.

Run the orchestrator:
```bash
python lakehouse/orchestration/super_orchestrator.py
```

This will:
- Set up Spark and Delta Lake
- Configure paths and environment
- Run the orchestrator to execute the defined pipelines (see `target_pipelines` in the script)

### Configuration
You can modify parameters in `super_orchestrator.py` and the `orchestrate` method:
- `warehouse_dir`, `external_path`, `catalog_name`, `managed`, `environment`
- `target_pipelines`: List of pipelines to run (e.g., `['fact_bike_status', 'dim_bike_station']`)
- `loading_mode`: How to discover and load pipeline files (default: `'file'`).
- `orchestration_mode`: Order of processing pipeline groups:
    - `'process_first'`: Roots to leaves (upstream to downstream).
    - `'process_last'`: Leaves to roots (downstream to upstream).
- `direction`: Which part of the dependency graph to process relative to targets:
    - `'upstream'`: Only dependencies of the targets (and the targets themselves).
    - `'downstream'`: Only dependents of the targets (and the targets themselves).
    - `'all'`: Both upstream and downstream pipelines (full subgraph).
    - `'none'`: Only the specified target_pipelines, with no dependencies.
- `parallelize_groups`: If `True`, pipelines within each group are run in parallel threads. If `False`, run serially.
- `fail_fast`: If `True`, stop execution as soon as any pipeline fails. If `False`, log errors and continue.
- `skip_downstream_on_failure`: If `True`, a pipeline will be skipped if all of its upstream dependencies have failed or been skipped.

## Extending the Project

- **Add new data sources**: Create new scripts in `ingestion/` and register them in the orchestrator.
- **Add new models**: Implement new fact/dimension tables in `modelisation/`.
- **Customize orchestration**: Adjust pipeline order, parallelism, and error handling in `super_orchestrator.py`.

## Troubleshooting & FAQ
- Ensure Spark and Delta Lake dependencies are installed and compatible.
- Data files and warehouse directories must be accessible and writable.
- For API-based ingestion, check network access and API limits.

## Credits

- Built with PySpark, Delta Lake, and open data APIs.
- Loïc Magnien