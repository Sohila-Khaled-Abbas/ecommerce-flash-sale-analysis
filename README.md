# Ecommerce Flash Sale Analysis & Bot Detection

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-FFFFFF?style=for-the-badge&logo=apachespark&logoColor=#E35A16)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00A9E0?style=for-the-badge&logo=databricks&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge)

## 📌 The Business Problem
E-commerce companies often rely on heavy discounts (flash sales) to acquire users. The standard assumption is that these users will return and become profitable in the long term. This is frequently false. 

This project investigates if recent flash sales are actually attracting **"deal snipers"** (users who buy once at a massive loss to the company and never return) and identifies potential **bot coordination** (multiple rapid transactions from the same IPs attempting to hoard limited-inventory discounted items). Standard KPI dashboards typically fail to catch this, simply showing a spike in volume and active users while masking margin erosion and anomalous transaction velocity.

---

## 🏗 Why Databricks?

Databricks was chosen as the core execution environment for this project for several critical reasons:
1. **Distributed Computing for Scale**: The data generation script creates 6.5 million synthetic rows. Single-node Python would be highly inefficient for this scale. Databricks leverages Apache Spark to distribute the workload, generating and processing this data in seconds.
2. **Delta Lake Integration**: Databricks natively supports Delta Lake, providing ACID transactions, scalable metadata handling, and time travel. This guarantees data reliability during our ad-hoc analysis.
3. **Advanced Windowing & Analytics**: Our analysis requires complex sliding windows (e.g., detecting transactions within a 60-second rolling window across millions of rows) which Spark SQL handles extremely efficiently compared to Pandas.
4. **Notebook-Driven Collaboration**: Databricks notebooks allow analysts and engineers to seamlessly collaborate, mix PySpark with markdown, and visualize data on the fly.

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
4. The script will generate 6.5 million rows of e-commerce data with simulated flash sales and bot behavior, saving it to a Delta table at `/tmp/ecommerce_transactions_delta`.

### Step 2: Ad-Hoc Analysis
1. Create a second notebook in your Databricks workspace.
2. Copy the contents of `scripts/ad_hoc_analysis.py` into the notebook.
3. Execute the script to perform:
    - **Cohort Profitability Analysis**: Identifies users acquired via flash sales and calculates their true lifetime margin vs. organic users.
    - **Bot Exploitation Detection**: Uses a 60-second rolling window to flag IPs with anomalous transaction velocity during flash sales.

---

## 📂 Project Structure
- `scripts/`: Contains the core Databricks PySpark scripts.
- `docs/`: Includes data lineage documentation and architecture diagrams.

## 📄 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
