# Oil & Gas Data Engineering Project

An end-to-end Data Engineering project built using **Microsoft Fabric** to process, transform, model, and analyze Brazilian oil and gas production data.

## Project Overview

This project demonstrates a complete **ELT (Extract, Load, Transform)** workflow, starting from raw SQLite data and transforming it into clean, structured, and analysis-ready datasets.

The project follows the **Medallion Architecture** and uses Microsoft Fabric components to build a complete data pipeline from raw data ingestion to the final analytical report.

## Architecture

**Data Source → Bronze → Silver → Gold → Semantic Model → Power BI Report**

### Bronze Layer
Stores the raw SQLite source data in the Microsoft Fabric Lakehouse without major transformations.

### Silver Layer
Cleans and standardizes the raw data using **PySpark**, including data type validation, missing-value handling, duplicate checks, and malformed numeric-value processing.

### Gold Layer
Transforms the cleaned data into analysis-ready **Fact and Dimension tables** following a **Star Schema**.

## Technologies

- Microsoft Fabric
- OneLake
- Lakehouse
- Data Pipelines
- PySpark
- Spark Notebooks
- Delta Tables
- Semantic Model
- Power BI
- SQLite

## Data Processing

The project includes:

- Data ingestion from a SQLite database
- Data type validation and conversion
- Missing-value analysis and handling
- Duplicate checks
- Cleaning malformed numeric values
- Data transformation using PySpark
- Bronze, Silver, and Gold data layers
- Delta table creation
- Star Schema modeling
- Automated daily Pipeline schedule
- Pipeline execution and validation

## Data Model

The Gold layer follows a **Star Schema** consisting of one Fact table and three Dimension tables:

- `gold_fact_production`
- `gold_dim_date`
- `gold_dim_field`
- `gold_dim_well`

The Dimension tables are connected to the Fact table using **one-to-many relationships**, enabling efficient filtering and analysis across production data.

## Dashboard

The final Power BI report contains three analytical pages:

### 1. Production Overview
Provides an overview of oil, water, and associated gas production, total wells, production trends over time, and top producing fields.

### 2. Field & Well Analysis
Focuses on field and well performance, oil production by field and state, and interactive filtering by field and location.

### 3. Injection & Operations Analysis
Analyzes operational injection metrics, including gas injection, secondary water injection, CO₂ injection, trends over time, and top fields by gas injection.

## Dataset

The project uses the **Brazil Oil and Gas Production** dataset available on Kaggle.

[View Dataset on Kaggle](https://www.kaggle.com/datasets/luciodias/brazil-oil-production)

**Source Format:** SQLite Database

**Source File:** `brazil_oil_production.sqlite`

## Project Files

This repository includes:

- Jupyter Notebook (`.ipynb`) — Data cleaning, transformation, and Gold layer creation
- Final Report (`.pdf`) — Final analytical dashboard
- Project Documentation (`.pdf`) — Project architecture and implementation summary

## Project Workflow

`SQLite Source → Bronze → Silver → Gold → Star Schema → Semantic Model → Power BI Report`

---

This project demonstrates the implementation of an end-to-end Data Engineering workflow using Microsoft Fabric, from raw data processing to an analysis-ready dimensional model and final interactive report.
