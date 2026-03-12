# Crypto Market ETL Pipeline (Databricks Lakehouse)

## Overview

This project implements an **end-to-end ETL pipeline** that ingests cryptocurrency market data from the public API provided by CoinGecko and processes it using **Databricks and PySpark**.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** to transform raw API data into analytics-ready datasets stored in **Delta Lake tables**.

The goal of the project is to demonstrate **data engineering practices such as API ingestion, data cleaning, transformation, and analytical modeling** in a modern lakehouse environment.

---

## Architecture
```
CoinGecko API

      ↓
Databricks Notebook (Extraction)

      ↓
Bronze Layer (Raw Data)

      ↓
Silver Layer (Cleaned Data)

      ↓
Gold Layer (Analytics Tables)

      ↓
SQL Analysis / Dashboard
```
---
## Medallion Data Layers

### Bronze Layer

Raw cryptocurrency market data is ingested directly from the API.

Example table:

bronze.brz_crypto

---
### Silver Layer

Data is cleaned and normalized for further processing.

Example table:

silver.slv_crypto

---

### Gold Layer

Analytics-ready datasets optimized for business queries.

Example tables:

gold.tbl_market

gold.tbl_top_crypto

gold.tbl_liquidity

These tables support queries such as:

- top cryptocurrencies by market capitalization

- market liquidity analysis

- daily price changes

---

## Example Analytics

### Top Cryptocurrencies by Market Cap

```
%sql

SELECT name, market_cap
FROM gold.crypto_top10
ORDER BY market_cap DESC
```

---
**Market Liquidity Metric**

volume_marketcap_ratio = total_volume / market_cap

This metric helps identify **highly traded assets relative to their market size**.

---

## Technologies Used

| Technology | Purpose                  |
| ---------- | ------------------------ |
| Databricks | Data processing platform |
| PySpark    | Data transformations     |
| Delta Lake | Storage format           |
| Python     | API ingestion            |
| SQL        | Analytical queries       |

---

## Project Structure
```
databricks-api-etl/
│
├── notebooks
│   ├── extract_api_data_to_bronze_layer
│   ├── silver_layer
│   ├── gold_layer
│
├── etl_pipeline.png
│   
│
│
└── README.md
```
---
## Pipeline Workflow

- Extract data from API
- Load raw dataset into Bronze tables
- Clean and transform into Silver tables
- Build analytical datasets in Gold tables

![Databricks ETL Pipeline](etl_pipeline.png)

---

## Example Output

```
coin	 | price     |market_cap	|total_volume
-----------------------------------------------------
Bitcoin  | 61023     |1220604703956	|40279221528
Ethereum | 1789.91   |216013372454	|17271659829
Tether   | 0.866693  |159435513672	|62284450900
```

---

## Author

Data Engineering portfolio project built to demonstrate modern lakehouse data pipeline development using Databricks.