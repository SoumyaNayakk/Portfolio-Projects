# Common Data Engineering Scenarios

This repository contains examples of common scenarios encountered in data engineering projects. Each scenario represents a typical challenge that data engineers need to address in real-world workflows.

---

## Scenario 1: Product Catalog Upsert

A company receives daily updates for its product catalog, including new products, price changes, and discontinued items. Instead of overwriting the entire catalog or simply appending new records, they need to **upsert the incoming data** (update existing products with the latest info and insert new products).

---

## Scenario 2: Incremental Sales Data Load

A company receives daily sales transaction files from multiple store locations in an Azure Data Lake folder. Instead of reprocessing all historical records every day, the solution should **incrementally load newly arrived files into a Delta table** to ensure timely updates to analytical dashboards.

---

## Scenario 3: Flatten Nested JSON Orders

An e-commerce platform receives customer order details from a mobile application in JSON format. The JSON contains **nested fields** such as customer info, payment details, and the list of purchased items. This data needs to be **flattened using PySpark** for further processing and analysis.

---

## Scenario 4: Reusable Customer Transformation Functions

Many PySpark notebooks require the same set of customer transformation functions, such as data formatting, null handling, and data validation. A solution is to **create a Python class to store these reusable functions** for consistency and code reusability.

---

## Scenario 5: Slowly Changing Dimensions (SCD Type 2)

A company maintains a product catalog in its data warehouse. Product details like name, category, and price may change over time. To preserve **historical data for reporting**, it is necessary to implement **SCD Type 2** logic to track changes in product attributes over time.

---

## Scenario 6: Real-Time Data Pipeline with Quality Checks

Records are processed from many locations in different formats. They need to be **cleaned, validated, and aggregated** for business reporting. Using **Delta Live Tables (DLT) pipelines**, the raw data is ingested from cloud storage, transformed with quality checks, and stored in Delta tables for analytical dashboards, ensuring **real-time reliability and accuracy**.

---