# 🚀 Databricks x DBT: End-to-End Data Engineering Project

This project simulates a **real-world enterprise-grade data pipeline** using **Databricks**, **DBT**, and the **Medallion Architecture** (Bronze, Silver, Gold layers). It features **incremental ingestion**, **structured streaming**, **dimensional modeling**, and **slowly changing dimensions (SCD)**, ending with SQL-based business logic transformation via **DBT**


---

## 🎯 Objectives

- Build a **cloud-native, modular data pipeline** using **Databricks & DBT**
- Implement **incremental data ingestion** using Autoloader and Spark Structured Streaming
- Transform data using **Delta Live Tables (DLT)** across Bronze, Silver, and Gold layers
- Implement **SCD Type 2** for dimension tracking
- Model business data using **DBT** with version control, testing, and modularity
- Simulate **production-ready validation** and orchestration

---

## 🛠️ Tools & Technologies

| Tool/Tech            | Purpose                                                       |
|----------------------|---------------------------------------------------------------|
| **Databricks**       | Unified data analytics platform for large-scale processing    |
| **PySpark**          | Distributed processing & streaming ingestion                  |
| **Autoloader**       | Incremental file ingestion into Bronze layer                  |
| **Delta Lake**       | ACID-compliant data lakehouse storage                         |
| **Delta Live Tables**| Declarative transformation pipelines for Silver & Gold layers |
| **DBT (Data Build Tool)** | SQL-based transformation, testing, and modeling            |
| **Git**              | Version control for DBT models and notebooks                  |

---

## 📐 Architecture Overview

```text
           ┌────────────┐
           │  Raw Files │ (CSV/JSON)
           └─────┬──────┘
                 │
         [Autoloader + Streaming]
                 ▼
          ┌────────────┐
          │  Bronze    │ → Raw ingested data
          └────────────┘
                 │
          [Delta Live Tables]
                 ▼
          ┌────────────┐
          │  Silver    │ → Cleaned & joined data
          └────────────┘
                 │
        [DLT + SCD2 Handling]
                 ▼
          ┌────────────┐
          │  Gold      │ → Star schema: facts & dimensions
          └────────────┘
                 │
          [DBT SQL Models + Tests]
                 ▼
     Final curated tables for business use
