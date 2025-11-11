# Sales Analytics ETL Pipeline — Databricks Lakehouse

## Overview
This project showcases an **end-to-end data engineering solution** for bank sales analytics built using **Databricks Lakehouse (AWS)** with **Lakeflow Declarative Pipelines** (formerly Delta Live Tables).  
The pipeline automates the ingestion, transformation, and aggregation of transactional sales data to deliver **real-time, reliable, and analytics-ready datasets** for business intelligence and decision-making.

By leveraging **Medallion Architecture**, the system ensures data quality, governance, and scalability—empowering business teams to monitor performance, optimize product strategy, and increase revenue visibility.

---

## Business Value
- **Revenue Intelligence:** Enabled executives to monitor regional and product-level sales performance in real time.  
- **Customer Insights:** Empowered marketing teams with customer segmentation and lifetime value analytics.  
- **Operational Efficiency:** Automated manual ETL processes, reducing data preparation time by ~60%.  
- **Regulatory Readiness:** Provided traceable, auditable data flows aligned with financial data governance standards.  
- **Decision Support:** Improved agility in pricing, promotions, and sales forecasting through reliable analytics pipelines.

## Architecture

###  Medallion Architecture
The pipeline is designed around the **Bronze–Silver–Gold** pattern for data lakehouse engineering:

- **Bronze Layer (Raw Ingestion)**  
  Ingests raw transactional and dimensional data from multiple banking systems.

- **Silver Layer (Cleansed & Enriched Data)**  
  Applies transformations and business rules to produce clean, enriched datasets.

- **Gold Layer (Aggregated Views)**  
  Delivers curated data models and materialized views for reporting and advanced analytics.

---

###  Source Tables
- `sales_east` and `sales_west` — Regional Transactional sales fact table containing time-series sales records.  
- `products` — Product dimension table with product metadata and category hierarchy.  
- `dim_customers` — Customer dimension table with demographic and regional attributes.

---

### Transformation Logic
- Performs **inner joins** between fact and dimension tables to enrich sales data.  
- Filters and selects key analytical fields (`region`, `category`, `total_amount`).  
- Implements **business rules** for data validation and quality assurance.  
- Standardizes schema and naming conventions for interoperability across datasets.



### Slowly Changing Dimensions (SCD)
Both **SCD Type 1** and **SCD Type 2** are implemented:
- **SCD Type 1:** For non-critical updates (e.g., name corrections).  
- **SCD Type 2:** For maintaining a **full historical record** of product, region, and customer changes using Databricks’ `create_auto_cdc_flow` functionality (formerly `apply_changes`).

This ensures the analytics layer can provide both current and historical insights with complete auditability.


###  Materialized Views
The pipeline generates two optimized datasets:
- **`business_sales`** — Aggregated sales metrics by region, product category, and time period.  
- **`customer_sales`** — Customer-level sales performance for segmentation and retention analysis.

These materialized views support:
- Near real-time dashboard updates  
- Ad-hoc analytics with low latency  
- Data-driven decision-making for sales and marketing teams  

## Key Features
- **Declarative Data Engineering:**  
  Built entirely with **Lakeflow Declarative Pipelines decorators**, improving code readability, maintainability, and auditability.

- **End-to-End Automation:**  
  Automatically refreshes data and updates views as new records arrive.

- **Data Quality & Governance:**  
  Built-in schema enforcement, SCD tracking, and metadata management ensure high data reliability.

- **Scalable Cloud-Native Design:**  
  Optimized for **Databricks on AWS**, allowing horizontal scaling across large banking datasets.







---

## Tech Stack
- Data Platform: Databricks on AWS

- Pipeline Framework: Databricks Lakeflow (Delta Live Tables)

- Core Storage: Delta Lake with Unity Catalog

- Data Ingestion: Auto Loader (cloud_files)

- Change Data Capture: Lakeflow create_auto_cdc_flow for SCD Type 1 & 2

- Core Logic: Spark SQL & Python (PySpark)

## Usage

1. **Pipeline Deployment**
   - Import the notebook and deploy using **Databricks Lakeflow Declarative Pipelines**.
   - Configure AWS workspace and access credentials to the data sources.

2. **Data Refresh**
   - Materialized views (`business_sales`, `customer_sales`) update automatically as new data is ingested.

3. **Analytics & Visualization**
   - Connect Databricks SQL, Power BI, or Tableau to the Gold layer for reporting.
   - Query `business_sales` for business KPIs and `customer_sales` for customer behavior analytics.

---

## Example Insights
| Metric | Description | Business Impact |
|:--|:--|:--|
| Regional Sales Trends | Aggregated revenue by geography | Informs regional marketing strategies |
| Product Category Performance | Identifies top-performing product lines | Guides inventory and pricing decisions |
| Customer Segmentation | Groups customers by spend and frequency | Enables targeted retention campaigns |

---


## Author
**Francis Xavier Inyangat**  
AI Data Engineer | BI Engineer 
📧 [fxinyangat.com](https://fxinyangat.com/)  • [LinkedIn](https://linkedin.com/in/inyangatfx)

