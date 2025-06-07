# Lakehouse x SuperLake

[![PyPI version](https://badge.fury.io/py/superlake.svg)](https://pypi.org/project/superlake/)

This repository is an **example project** designed to show how to build a project leveraging the [superlake](https://pypi.org/project/superlake/) library. It implements a modular Lakehouse architecture for ingesting, transforming, modeling, and orchestrating data pipelines using PySpark and Delta Lake. It supports multi-source data ingestion (ERP, CRM, open data), transformation into bronze/silver/gold tables, and dimensional modeling for analytics and reporting.

## Table of Contents
- [Overview](#overview)
   - [Repository Structure](#repository-structure)
   - [Installation & Requirements](#installation--requirements)
   - [Quickstart](#quickstart)
   - [How to run](#how-to-run)
   - [Configuration](#configuration)
   - [Extending the Project](#extending-the-project)
   - [Troubleshooting & FAQ](#troubleshooting--faq)
- [Architecture](#architecture)
   - [Ingestion](#ingestion)
   - [Modeling](#modeling)
   - [Orchestration](#orchestration)
   - [Cataloguing](#cataloguing)



<br/><br/><br/>

# Overview

## Repository Structure

```
.
├── lakehouse/                              # Main source code
│   ├── action/                             # Orchestration, catalog, and configuration logic
│   │   ├── config.py                       # Centralized configuration
│   │   ├── super_orchestrator.py           # Main entry point for orchestration
│   │   └── super_cataloguer.py             # Main entry point for catalog operations (Unity Catalog)
│   ├── ingestion/                          # Data ingestion pipelines (ERP, CRM, Open Data)
│   │   ├── erp/
│   │   │   └── erp_sales_transactions.py
│   │   ├── crm/
│   │   │   └── crm_contacts.py
│   │   └── open/
│   │       ├── velib_station_info.py
│   │       ├── velib_station_status.py
│   │       ├── velov_station_info.py
│   │       └── velov_station_status.py
│   ├── modelisation/                       # Data modeling (facts, dimensions)
│   │   ├── facts/
│   │   │   ├── fact_sales.py
│   │   │   └── fact_bike_status.py
│   │   └── dimensions/
│   │       ├── dim_bike_station.py
│   │       ├── dim_customer.py
│   │       ├── dim_date.py
│   │       ├── dim_product.py
│   │       ├── dim_promo.py
│   │       └── dim_store.py
│   └── utils/                              # Utility scripts (e.g., Opendatasoft loader)
│       └── opendatasoft.py
```

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

## Quickstart

1. Clone the repository and install dependencies as above.
2. Configure your environment in [`lakehouse/action/config.py`](lakehouse/action/config.py).
3. Run the orchestrator to execute all pipelines:
   ```bash
   python lakehouse/action/super_orchestrator.py
   ```
4. Run catalog operations (comments, keys, etc.):
   ```bash
   python lakehouse/action/super_cataloguer.py
   ```
5. Explore the generated Delta tables and catalog in your Spark environment.

## How to Run

The main entry points are:

**Orchestrator:**
```bash
python lakehouse/action/super_orchestrator.py
```
Runs the full pipeline orchestration.

**Catalog Operations (Unity Catalog logic):**
```bash
python lakehouse/action/super_cataloguer.py
```
Applies table/column comments, manages primary/foreign keys, and ensures catalog quality for Unity Catalog tables.

## Configuration

Both orchestrator and catalog scripts use `lakehouse/action/config.py` for configuration. 

## Extending the Project

- **Add new data sources**: Create new scripts in `ingestion/`.
- **Add new models**: Implement new fact/dimension tables in `modelisation/`.
- **Customize orchestration**: Adjust pipeline order, parallelism in `action/super_orchestrator.py` 
- **Catalog**: Run the `action/super_cataloguer.py` to update the Unity Catalog with all structure and metadata.

## Troubleshooting & FAQ
- Ensure Spark and Delta Lake dependencies are installed and compatible.
- Data files and warehouse directories must be accessible and writable.
- For API-based ingestion, check network access and API limits.

<br/><br/><br/>

# Architecture

## Ingestion

Data is ingested from multiple sources into bronze and silver. The ingestion pipelines are built using the SuperDeltaTable class to manage the behaviour of the bronze and silver tables. 
They must return:
- The bronze and silver tables in SuperDeltaTable format
- The Change Data Capture returning a Spark DataFrame
- The Transformation function returning a Spark DataFrame
- The Deletion function returning the rows to delete from the Silver table if any

They run using the SuperPipeline class. 
- **ERP**: [`erp_sales_transactions.py`](lakehouse/ingestion/erp/erp_sales_transactions.py) — Loads sales transactions (generated).
- **CRM**: [`crm_contacts.py`](lakehouse/ingestion/crm/crm_contacts.py) — Loads contact/customer data (generated).
- **Open Data**: [`velib_station_status.py`](lakehouse/ingestion/open/velib_station_status.py), [`velov_station_status.py`](lakehouse/ingestion/open/velov_station_status.py), etc. — Loads bike station data from public APIs.

## Modeling

Data is modeled from silver into gold. The modeling pipelines take data from the silver layer (usually) and remodel the data into the gold layer.
The modeling can vary depending on project and reporting requirements; this repo uses a Kimball modeling style. 
The modeling pipelines are simpler, they return:
- The sink table where to save the data
- The function to generate the data to sink in the table

They are used for Dimensions and Fact tables:
- **Fact Tables**: e.g., [`fact_sales.py`](lakehouse/modeling/facts/fact_sales.py), [`fact_bike_status.py`](lakehouse/modeling/facts/fact_bike_status.py)
- **Dimension Tables**: e.g., [`dim_customer.py`](lakehouse/modeling/dimensions/dim_customer.py), [`dim_product.py`](lakehouse/modeling/dimensions/dim_product.py), [`dim_bike_station.py`](lakehouse/modeling/dimensions/dim_bike_station.py), [`dim_date.py`](lakehouse/modeling/dimensions/dim_date.py), etc.

## Orchestration

[`super_orchestrator.py`](lakehouse/action/super_orchestrator.py) is the main entry point. Sets up Spark, logging, tracing, and runs the SuperOrchestrator to execute pipelines (via `SuperOrchestrator.orchestrate`).


## Cataloguing

[`super_cataloguer.py`](lakehouse/action/super_cataloguer.py): Main entry point for catalog operations (comments, keys, etc.), especially for Unity Catalog tables (via `SuperCataloguer.feature_name`).