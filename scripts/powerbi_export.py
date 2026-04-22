# -*- coding: utf-8 -*-
"""
VoltEdge Electronics — Power BI Export Pipeline
================================================
This script is designed to run in a Databricks notebook AFTER both
`data_generation.py` and `ad_hoc_analysis.py` have been executed.

It materializes three analytics-ready tables optimized for Power BI
consumption via Databricks Partner Connect or exported CSV/Parquet files.

Tables produced:
    1. fact_transactions       — Full enriched transaction fact table
    2. agg_cohort_profitability — Pre-aggregated cohort × category margin analysis
    3. agg_bot_detection       — Pre-aggregated anomalous IP forensic report
"""

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# ============================================================================
# Configuration
# ============================================================================
SOURCE_PATH = "/tmp/ecommerce_transactions_delta"
EXPORT_BASE = "/tmp/voltedge_powerbi_exports"

# ============================================================================
# 1. Load Source Delta Table
# ============================================================================
df = spark.read.format("delta").load(SOURCE_PATH)

print("=" * 60)
print("VOLTEDGE ELECTRONICS — POWER BI EXPORT PIPELINE")
print("=" * 60)

# ============================================================================
# 2. Fact Table: Enriched Transactions
# ============================================================================
# Add derived time-intelligence columns that Power BI can use for slicers
# without requiring complex DAX. This reduces report-time computation.

fact_transactions = df \
    .withColumn("transaction_date", F.to_date("transaction_timestamp")) \
    .withColumn("transaction_hour", F.hour("transaction_timestamp")) \
    .withColumn("day_of_week", F.dayofweek("transaction_timestamp")) \
    .withColumn("day_name", F.date_format("transaction_timestamp", "EEEE")) \
    .withColumn("week_number", F.weekofyear("transaction_timestamp")) \
    .withColumn("month_name", F.date_format("transaction_timestamp", "MMMM")) \
    .withColumn("is_margin_negative", F.when(F.col("margin_usd") < 0, True).otherwise(False)) \
    .withColumn("discount_tier",
                F.when(F.col("discount_rate") >= 0.6, "Deep Discount (60%+)")
                 .when(F.col("discount_rate") >= 0.4, "Heavy Discount (40-60%)")
                 .when(F.col("discount_rate") >= 0.1, "Moderate Discount (10-40%)")
                 .otherwise("Minimal / None (0-10%)")) \
    .withColumn("account_age_bucket",
                F.when(F.col("account_age_days") <= 2, "New (0-2 days)")
                 .when(F.col("account_age_days") <= 30, "Recent (3-30 days)")
                 .when(F.col("account_age_days") <= 90, "Established (31-90 days)")
                 .otherwise("Veteran (90+ days)"))

fact_transactions.write.format("delta").mode("overwrite") \
    .save(f"{EXPORT_BASE}/fact_transactions")

print(f"[✓] fact_transactions — {fact_transactions.count():,} rows exported")

# ============================================================================
# 3. Aggregate Table: Cohort Profitability by Product Category
# ============================================================================
# Pre-compute the cohort LTV analysis so Power BI only needs to visualize,
# not recalculate, the window functions across 6.5M rows.

user_window = Window.partitionBy("user_id").orderBy("transaction_timestamp")
df_ranked = df.withColumn("txn_rank", F.row_number().over(user_window))

acquisition_cohorts = df_ranked.filter(F.col("txn_rank") == 1) \
    .select("user_id", F.col("is_flash_sale").alias("acquired_via_flash_sale"))

user_ltv = df.groupBy("user_id").agg(
    F.sum("margin_usd").alias("lifetime_margin"),
    F.count("transaction_id").alias("total_purchases"),
    F.min("transaction_timestamp").alias("first_purchase_date"),
    F.max("transaction_timestamp").alias("last_purchase_date"),
    F.countDistinct("product_category").alias("categories_purchased")
)

user_ltv = user_ltv.withColumn(
    "days_active",
    F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date"))
)

cohort_profitability = user_ltv.join(acquisition_cohorts, "user_id") \
    .join(df.select("user_id", "product_category").distinct(), "user_id") \
    .groupBy("acquired_via_flash_sale", "product_category") \
    .agg(
        F.count("user_id").alias("user_count"),
        F.avg("total_purchases").alias("avg_purchases_per_user"),
        F.avg("lifetime_margin").alias("avg_lifetime_margin_usd"),
        F.sum("lifetime_margin").alias("total_cohort_margin"),
        F.avg("days_active").alias("avg_days_active"),
        F.avg("categories_purchased").alias("avg_categories_purchased"),
        F.percentile_approx("lifetime_margin", 0.5).alias("median_lifetime_margin")
    ).orderBy("acquired_via_flash_sale", "product_category")

cohort_profitability.write.format("delta").mode("overwrite") \
    .save(f"{EXPORT_BASE}/agg_cohort_profitability")

print(f"[✓] agg_cohort_profitability — {cohort_profitability.count():,} rows exported")

# ============================================================================
# 4. Aggregate Table: Bot Detection Report
# ============================================================================
# Materializes the anomalous IP report with all forensic indicators.

flash_sales_df = df.filter(F.col("is_flash_sale") == True)

velocity_window = Window.partitionBy("ip_address") \
    .orderBy(F.col("transaction_timestamp").cast("long")) \
    .rangeBetween(-60, 0)

bot_detection = flash_sales_df.withColumn(
    "txns_in_last_60s", F.count("transaction_id").over(velocity_window)
)

bot_report = bot_detection.filter(F.col("txns_in_last_60s") > 5) \
    .groupBy("ip_address") \
    .agg(
        F.max("txns_in_last_60s").alias("peak_txns_per_minute"),
        F.countDistinct("user_id").alias("unique_accounts_used"),
        F.avg("account_age_days").alias("avg_account_age_days"),
        F.min("account_age_days").alias("min_account_age_days"),
        F.count("transaction_id").alias("total_suspicious_txns"),
        F.sum("margin_usd").alias("margin_impact_usd"),
        F.sum("final_price").alias("total_revenue_captured"),
        F.countDistinct("product_category").alias("categories_targeted"),
        F.collect_set("payment_method").alias("payment_methods_used"),
        F.collect_set("shipping_speed").alias("shipping_speeds_used")
    ).orderBy(F.col("peak_txns_per_minute").desc())

# Flatten array columns for Power BI compatibility (no nested types)
bot_report_flat = bot_report \
    .withColumn("payment_methods", F.concat_ws(", ", "payment_methods_used")) \
    .withColumn("shipping_speeds", F.concat_ws(", ", "shipping_speeds_used")) \
    .drop("payment_methods_used", "shipping_speeds_used")

bot_report_flat.write.format("delta").mode("overwrite") \
    .save(f"{EXPORT_BASE}/agg_bot_detection")

print(f"[✓] agg_bot_detection — {bot_report_flat.count():,} rows exported")

# ============================================================================
# 5. Summary
# ============================================================================
print("\n" + "=" * 60)
print("EXPORT COMPLETE")
print("=" * 60)
print(f"All tables written to: {EXPORT_BASE}/")
print("""
Power BI Connection Options:
  1. Databricks Partner Connect  → Direct live connection
  2. ODBC/JDBC                   → Connect via Databricks SQL Warehouse
  3. Export to CSV                → Use dbutils.fs to copy to local storage
""")
