# Iowa Liquor Sales ETL & Dimensional Modeling

A PySpark data engineering pipeline developed in Databricks to transform the public State of Iowa Liquor Sales dataset into a dimensional star schema, exported to Delta Lake, CSV, and JSON formats.

## Overview

This project implements a complete batch ETL workflow using Apache Spark (PySpark):
1. **Extraction & Schema Enforcement**: Ingests raw Iowa liquor transaction records with strongly typed `StructType` schemas.
2. **Data Cleaning & Standardization**: Cleanses store locations, capitalizes text fields (`initcap`), normalizes zip codes, and generates synthetic surrogate keys.
3. **Dimensional Modeling (Star Schema)**:
   - **Fact Tables**:
     - `Iowa_FactSales`: Transactional facts including sales volume, revenue, bottle counts, retail cost, and date partition keys.
     - `Iowa_FactPopulation`: County-level population benchmarks for per-capita analytics.
   - **Dimension Tables**:
     - `Iowa_DimStore`: Store details (name, address, city, county, zip code).
     - `Iowa_DimItem`: Unique item catalog with deduplication and surrogate handling.
     - `Iowa_DimCategory`: Liquor categories.
     - `Iowa_DimVendor`: Beverage vendors and distributors.
     - `Iowa_DimDate`: Generated calendar dimension supporting standard and fiscal year hierarchies (October-to-September and July-to-June).
4. **Multi-Format Storage**:
   - **Delta Lake**: Partitioned ACID tables with schema evolution enabled (`overwriteSchema`).
   - **CSV**: Coalesced single-file dimension and fact extracts for downstream reporting.
   - **JSON**: Sampled record exports for API integration or streaming ingestion simulations.

## Repository Contents

| File | Description |
| --- | --- |
| `20240406_iowa_liquor.ipynb` | Databricks interactive notebook with all ETL steps and documentation cells. |
| `20240406_iowa_liquor.py` | Standalone PySpark script exported from the Databricks notebook. |

## Prerequisites & Technologies

- **Runtime**: Apache Spark / Databricks Runtime (DBR 10.x+)
- **Language**: Python 3.x / PySpark
- **Storage**: Delta Lake, DBFS / Cloud Object Storage (Azure Blob / ADLS Gen2, AWS S3)
