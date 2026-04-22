# VoltEdge Electronics: Scalper Bot Analytics

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-FFFFFF?style=for-the-badge&logo=apachespark&logoColor=#E35A16)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00A9E0?style=for-the-badge&logo=databricks&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge)

## 📌 The Business Problem
**VoltEdge Electronics** is a high-end consumer electronics retailer running highly anticipated "Cyber Flash Drops" for next-gen consoles, GPUs, and premium peripherals. While these events generate massive traffic spikes, the standard assumption that these spikes lead to long-term profitable customers is proving false.

VoltEdge is under siege by **Scalper Bots**. These coordinated bots use residential proxy networks to hoard limited-inventory items during flash drops, bypassing "1-per-customer" limits. 

This project is a forensic data engineering investigation to:
1. Prove that users acquired during "Cyber Flash Drops" (especially for GPUs and Consoles) have a deeply negative lifetime margin.
2. Detect advanced bot exploitation by correlating transaction velocity with low account ages, prepaid credit card usage, and overnight shipping preferences.

Standard KPI dashboards typically fail to catch this, simply showing a massive win in volume and active users while masking margin erosion and bot infiltration.

---

## 🏗 Why Databricks?

Databricks was chosen as the core execution environment for this project for several critical reasons:
1. **Distributed Computing for Scale**: The data generation script creates 6.5 million synthetic rows. Single-node Python would be highly inefficient for this scale. Databricks leverages Apache Spark to distribute the workload, generating and processing this data in seconds.
2. **Delta Lake Integration**: Databricks natively supports Delta Lake, providing ACID transactions, scalable metadata handling, and time travel. This guarantees data reliability during our ad-hoc analysis.
3. **Advanced Windowing & Analytics**: Our analysis requires complex sliding windows (e.g., detecting transactions within a 60-second rolling window across millions of rows) which Spark SQL handles extremely efficiently compared to Pandas.

---

## 🚀 Implementation Guide

### Prerequisites
- A Databricks workspace.
- A cluster with a standard Databricks Runtime (DBR 10.4 LTS or higher recommended).
- Appropriate permissions to write to DBFS (`/tmp/` by default in these scripts).

### Step 1: Data Generation
1. Open your Databricks Workspace and create a new notebook.
2. Copy the contents of `scripts/data_generation.py` into the notebook.
3. Attach the notebook to your cluster and run the cells.
4. The script will generate 6.5 million rows of e-commerce data with simulated flash sales and scalper bot behavior, saving it to a Delta table at `/tmp/ecommerce_transactions_delta`.

### Step 2: VoltEdge Analytics
1. Create a second notebook in your Databricks workspace.
2. Copy the contents of `scripts/ad_hoc_analysis.py` into the notebook.
3. Execute the script to perform:
    - **Cohort Profitability Analysis**: Identifies users acquired via flash sales segmented by product category, calculating their true lifetime margin vs. organic users.
    - **Bot Exploitation Detection**: Uses a 60-second rolling window to flag IPs with anomalous transaction velocity during flash sales, correlating them with account age, payment methods, and shipping preferences to prove malicious intent.

---

## 📂 Project Structure
- `scripts/`: Contains the core Databricks PySpark scripts.
- `docs/`: Includes data lineage documentation and architecture diagrams.

## 📄 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
