# VoltEdge Electronics — Power BI Dashboard Design

This document specifies the Power BI data model, DAX measures, and dashboard page layout for the VoltEdge Scalper Bot Analytics project.

---

## 1. Data Connection

### Recommended: Databricks Partner Connect
Power BI connects directly to the Delta tables via a live DirectQuery or Import connection through a Databricks SQL Warehouse.

| Table | Source Path | Import Mode | Refresh |
| :--- | :--- | :---: | :---: |
| `fact_transactions` | `/tmp/voltedge_powerbi_exports/fact_transactions` | Import | Daily |
| `agg_cohort_profitability` | `/tmp/voltedge_powerbi_exports/agg_cohort_profitability` | Import | Daily |
| `agg_bot_detection` | `/tmp/voltedge_powerbi_exports/agg_bot_detection` | Import | Daily |

### Alternative: ODBC/JDBC
Use the Databricks SQL Warehouse ODBC driver to connect Power BI Desktop directly.

### Alternative: CSV Export
Run in a Databricks notebook cell after the export script:
```python
# Export fact table to CSV
spark.read.format("delta").load("/tmp/voltedge_powerbi_exports/fact_transactions") \
    .coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("/tmp/voltedge_csv_exports/fact_transactions")
```

---

## 2. Data Model (Star Schema)

```
                    ┌──────────────────────────┐
                    │   agg_cohort_profitability│
                    │   ────────────────────────│
                    │   acquired_via_flash_sale │
                    │   product_category        │
                    │   user_count              │
                    │   avg_lifetime_margin_usd │
                    │   total_cohort_margin     │
                    │   median_lifetime_margin  │
                    └──────────────────────────┘

┌──────────────────────────┐          ┌──────────────────────────┐
│     fact_transactions    │          │    agg_bot_detection     │
│   ────────────────────── │          │   ────────────────────── │
│   transaction_id    (PK) │          │   ip_address        (PK) │
│   transaction_date       │          │   peak_txns_per_minute   │
│   user_id                │          │   unique_accounts_used   │
│   product_category       │          │   avg_account_age_days   │
│   base_price             │          │   margin_impact_usd      │
│   final_price            │          │   total_revenue_captured │
│   margin_usd             │          │   payment_methods        │
│   discount_tier          │          │   shipping_speeds        │
│   account_age_bucket     │          └──────────────────────────┘
│   is_flash_sale          │
│   is_margin_negative     │
│   day_name / month_name  │
└──────────────────────────┘
```

> **Note:** The three tables are intentionally denormalized for Power BI performance. `fact_transactions` serves as the central fact table. The two `agg_` tables are standalone summary tables used by dedicated report pages — no relationships are needed between them.

---

## 3. DAX Measures Reference

### KPI Card Measures
```dax
Total Revenue = SUM(fact_transactions[final_price])

Total Margin = SUM(fact_transactions[margin_usd])

Margin Rate % = DIVIDE([Total Margin], SUM(fact_transactions[base_price]), 0)

Flash Sale Revenue = 
    CALCULATE([Total Revenue], fact_transactions[is_flash_sale] = TRUE)

Flash Sale Margin = 
    CALCULATE([Total Margin], fact_transactions[is_flash_sale] = TRUE)

Loss Transactions % = 
    DIVIDE(
        COUNTROWS(FILTER(fact_transactions, fact_transactions[is_margin_negative] = TRUE)),
        COUNTROWS(fact_transactions),
        0
    )
```

### Cohort Comparison Measures
```dax
Organic Avg LTV = 
    CALCULATE(
        AVERAGE(agg_cohort_profitability[avg_lifetime_margin_usd]),
        agg_cohort_profitability[acquired_via_flash_sale] = FALSE
    )

Flash Sale Avg LTV = 
    CALCULATE(
        AVERAGE(agg_cohort_profitability[avg_lifetime_margin_usd]),
        agg_cohort_profitability[acquired_via_flash_sale] = TRUE
    )

LTV Gap = [Organic Avg LTV] - [Flash Sale Avg LTV]
```

### Bot Detection Measures
```dax
Total Suspicious IPs = COUNTROWS(agg_bot_detection)

Bot Margin Damage = SUM(agg_bot_detection[margin_impact_usd])

Avg Peak Velocity = AVERAGE(agg_bot_detection[peak_txns_per_minute])

New Account Bot % = 
    DIVIDE(
        COUNTROWS(FILTER(agg_bot_detection, agg_bot_detection[avg_account_age_days] <= 2)),
        COUNTROWS(agg_bot_detection),
        0
    )
```

---

## 4. Dashboard Pages

### Page 1: Executive Overview
**Purpose:** High-level KPIs for VoltEdge leadership.

| Visual | Type | Data |
| :--- | :---: | :--- |
| Total Revenue | KPI Card | `[Total Revenue]` |
| Total Margin | KPI Card | `[Total Margin]` with conditional formatting (red if negative) |
| Margin Rate % | KPI Card | `[Margin Rate %]` |
| Loss Transactions % | KPI Card | `[Loss Transactions %]` |
| Revenue vs Margin Over Time | Line Chart | X: `transaction_date`, Y₁: `final_price` SUM, Y₂: `margin_usd` SUM |
| Margin by Product Category | Stacked Bar | X: `product_category`, Y: `margin_usd` SUM, Legend: `is_flash_sale` |
| Discount Tier Distribution | Donut Chart | Legend: `discount_tier`, Values: COUNT of transactions |

### Page 2: Cohort Profitability Deep-Dive
**Purpose:** Side-by-side comparison of flash sale vs organic acquisition cohorts.

| Visual | Type | Data |
| :--- | :---: | :--- |
| Organic vs Flash LTV | Clustered Bar | Source: `agg_cohort_profitability`, X: `product_category`, Y: `avg_lifetime_margin_usd`, Legend: `acquired_via_flash_sale` |
| LTV Gap Card | KPI Card | `[LTV Gap]` — conditional red/green |
| User Count by Cohort | Stacked Column | X: `product_category`, Y: `user_count`, Legend: `acquired_via_flash_sale` |
| Avg Purchases per User | Grouped Bar | Cohort comparison of repeat purchase behavior |
| Median vs Mean LTV | Table | Side-by-side `avg_lifetime_margin_usd` vs `median_lifetime_margin` |

### Page 3: Bot Exploitation War Room
**Purpose:** Forensic IP-level analysis for the security/fraud team.

| Visual | Type | Data |
| :--- | :---: | :--- |
| Suspicious IPs Count | KPI Card | `[Total Suspicious IPs]` |
| Bot Margin Damage | KPI Card | `[Bot Margin Damage]` in red |
| New Account Bot % | KPI Card | `[New Account Bot %]` |
| Top 20 IPs by Velocity | Horizontal Bar | X: `peak_txns_per_minute`, Y: `ip_address` (Top N filter = 20) |
| Accounts per IP | Scatter Plot | X: `unique_accounts_used`, Y: `peak_txns_per_minute`, Size: `margin_impact_usd` |
| Forensic Detail Table | Matrix | Rows: `ip_address`, Columns: All bot metrics, conditional formatting on `avg_account_age_days` |

### Page 4: Time Intelligence
**Purpose:** Temporal patterns to identify when bots strike.

| Visual | Type | Data |
| :--- | :---: | :--- |
| Transactions by Hour | Area Chart | X: `transaction_hour`, Y: COUNT, Legend: `is_flash_sale` |
| Transactions by Day of Week | Column Chart | X: `day_name`, Y: COUNT |
| Flash Sale Volume by Week | Line Chart | X: `week_number`, Y: COUNT filtered to `is_flash_sale = TRUE` |
| Margin Heatmap | Matrix | Rows: `day_name`, Columns: `transaction_hour`, Values: SUM `margin_usd`, conditional formatting |

---

## 5. Color Theme

To maintain brand consistency with VoltEdge:

```json
{
  "name": "VoltEdge Dark",
  "dataColors": [
    "#8b5cf6",
    "#0ea5e9",
    "#10b981",
    "#f59e0b",
    "#ef4444",
    "#ec4899",
    "#06b6d4",
    "#a78bfa"
  ],
  "background": "#09090b",
  "foreground": "#f4f4f5",
  "tableAccent": "#8b5cf6"
}
```

Save as `VoltEdge_Dark.json` and import into Power BI via **View → Themes → Browse for themes**.
