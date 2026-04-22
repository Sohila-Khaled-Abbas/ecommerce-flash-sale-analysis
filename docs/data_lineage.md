# Data Lineage Document

This document traces the flow of data through the VoltEdge Electronics Scalper Analytics project, from raw synthetic generation to the final analytical aggregates.

## 1. Data Generation Phase
**Source Script:** `scripts/data_generation.py`
**Execution Engine:** Apache Spark (Databricks)

### 1.1 Process overview
The pipeline initiates by synthetically generating 6.5 million records representing e-commerce transactions for VoltEdge Electronics over the past 90 days. The data is generated entirely in memory across Spark partitions.

### 1.2 Transformations Applied
- **Base Range Generation:** `spark.range(6500000)` initializes the raw dataset.
- **Timestamp Computation:** Uses `current_timestamp()` and random offsets to distribute purchases.
- **Entity Simulation:** Random generation assigns users (`user_id`, 1.5M unique) and products (`product_id`, 50k unique).
- **VoltEdge Enrichment:** Assigns `product_category` (Gaming Consoles, GPUs, Peripherals, Smartphones), `account_age_days`, `payment_method`, and `shipping_speed`.
- **Financial Logic:** 
    - 15% of transactions are flagged as `is_flash_sale = True`.
    - `discount_rate` is assigned conditionally (40-80% for flash sales, 0-10% otherwise).
    - `margin_usd` is derived based on a 40% standard markup assumption minus the applied discount.
- **Network Metadata:** 5% of traffic is artificially routed through a small pool of 100 IPs (`192.168.1.x`) to simulate bot traffic, while the rest are randomized.

### 1.3 Sink Output
- **Format:** Delta Table
- **Location:** `dbfs:/tmp/ecommerce_transactions_delta`
- **Write Mode:** `overwrite`

---

## 2. Ad-Hoc Analysis Phase
**Source Script:** `scripts/ad_hoc_analysis.py`
**Execution Engine:** Apache Spark (Databricks)

### 2.1 Source Data
The analysis reads directly from the Delta table created in Phase 1 (`dbfs:/tmp/ecommerce_transactions_delta`).

### 2.2 Pipeline A: Cohort Profitability Analysis
This pipeline determines if flash sales erode long-term value based on product categories.
- **Step 1 (Windowing):** Partitions by `user_id` and orders by `transaction_timestamp` to find the first transaction per user (`txn_rank == 1`).
- **Step 2 (Tagging):** Creates an `acquisition_cohorts` table flagging users as `acquired_via_flash_sale` based on their first transaction.
- **Step 3 (Aggregation):** Computes total lifetime margin and purchase count per `user_id`.
- **Step 4 (Join & Final Aggregate):** Joins the lifetime aggregates with the acquisition cohorts and groups by `acquired_via_flash_sale` and `product_category` to produce the final metrics.

### 2.3 Pipeline B: Bot Exploitation Detection
This pipeline detects rapid transaction velocity indicative of inventory hoarding by scalper bots.
- **Step 1 (Filtering):** Narrows the dataset exclusively to `is_flash_sale == True`.
- **Step 2 (Time Windowing):** Creates a rolling 60-second window partitioned by `ip_address` using the UNIX epoch timestamp.
- **Step 3 (Velocity Calculation):** Computes `txns_in_last_60s` for every transaction.
- **Step 4 (Anomaly Detection):** Filters for records where velocity > 5 transactions/minute.
- **Step 5 (Final Aggregate):** Groups by `ip_address` to find the peak velocity, unique accounts used, average account age, and arrays of shipping/payment methods used by the bot cluster.

---

## 3. Power BI Export Phase
**Source Script:** `scripts/powerbi_export.py`
**Execution Engine:** Apache Spark (Databricks)

### 3.1 Process Overview
This phase materializes three analytics-ready Delta tables optimized for Power BI consumption. It pre-computes expensive window functions and aggregations so that Power BI only needs to visualize, not recalculate.

### 3.2 Tables Produced

#### `fact_transactions`
The full enriched transaction fact table with derived time-intelligence columns:
- `transaction_date`, `transaction_hour`, `day_of_week`, `day_name`, `week_number`, `month_name` — enable Power BI time slicers without DAX.
- `is_margin_negative` — boolean flag for conditional formatting.
- `discount_tier` — human-readable bucketing (Deep / Heavy / Moderate / Minimal).
- `account_age_bucket` — New (0-2 days), Recent (3-30), Established (31-90), Veteran (90+).

#### `agg_cohort_profitability`
Pre-aggregated cohort × product category margin analysis including:
- `avg_lifetime_margin_usd`, `total_cohort_margin`, `median_lifetime_margin`
- `avg_days_active`, `avg_categories_purchased`

#### `agg_bot_detection`
Flattened anomalous IP forensic report (no nested array types for Power BI compatibility):
- `peak_txns_per_minute`, `unique_accounts_used`, `avg_account_age_days`
- `margin_impact_usd`, `total_revenue_captured`
- `payment_methods` and `shipping_speeds` as comma-separated strings.

### 3.3 Sink Output
- **Format:** Delta Tables
- **Location:** `dbfs:/tmp/voltedge_powerbi_exports/{table_name}`
- **Write Mode:** `overwrite`

---

## 4. Visualization Phase
**Tool:** Microsoft Power BI
**Design Spec:** `docs/powerbi_dashboard_design.md`

### 4.1 Dashboard Pages
| Page | Purpose |
| :--- | :--- |
| Executive Overview | High-level KPIs: revenue, margin, loss transaction rate |
| Cohort Deep-Dive | Flash sale vs organic LTV comparison by product category |
| Bot War Room | IP-level forensic analysis with velocity and account age indicators |
| Time Intelligence | Temporal patterns: hourly, daily, and weekly transaction heatmaps |

### 4.2 Data Connection
Power BI connects to the materialized Delta tables via Databricks Partner Connect (recommended), ODBC/JDBC, or exported CSV files.

---

## 5. Data Dictionary (Delta Table)

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `transaction_id` | String | Unique UUID for the purchase |
| `transaction_timestamp` | Timestamp | Date and time of the purchase |
| `user_id` | Integer | Unique identifier for the customer |
| `product_id` | Integer | Unique identifier for the item |
| `product_category` | String | Category of the item (e.g., GPUs, Consoles) |
| `base_price` | Double | Retail price before discounts |
| `discount_rate` | Double | Percentage discount applied |
| `final_price` | Double | Paid price (`base_price` * (1 - `discount_rate`)) |
| `margin_usd` | Double | Profit/Loss to the company |
| `is_flash_sale` | Boolean | True if item was purchased during a Cyber Flash Drop |
| `ip_address` | String | Simulated IPv4 address of the purchaser |
| `account_age_days` | Integer | Age of the purchaser's account in days |
| `payment_method` | String | Payment method used (e.g., Prepaid Credit Card) |
| `shipping_speed` | String | Selected shipping speed (e.g., Overnight) |
