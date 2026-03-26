# Healthcare Claims Lakehouse

Built an end-to-end Healthcare Claims Lakehouse using PySpark and Delta Lake with Bronze-Silver-Gold architecture, schema-drift handling, reject management, SCD Type 2 provider dimension, core dimensional modeling, and Gold marts for claim cost, denial analysis, provider performance, member utilization, and turnaround metrics

---
### Author

Vaishnavi Mendre
Data Engineer | PySpark | SQL | Delta Lake | AWS | Data Bricks

---

## Project Overview

Healthcare organizations receive data from multiple source systems in different formats such as claims, member records, provider data, and payments.  
This project builds a scalable local lakehouse pipeline that:

- ingests raw source data into a Bronze layer
- standardizes and validates records in a Silver layer
- prepares curated, analytics-ready datasets in a Gold layer

The goal is to showcase practical data engineering concepts such as:

- medallion architecture
- incremental ingestion
- schema handling
- data quality validation
- deduplication
- dimensional modeling
- analytics-ready data preparation

---

## Objective

Build an end-to-end **Healthcare Claims Lakehouse** that demonstrates how a real-world healthcare data platform can be designed using local tools while following enterprise data engineering practices.

---

## Business Use Case

Healthcare claims data is often spread across multiple systems and arrives in different formats.  
Analytics teams need reliable, standardized, and curated datasets to answer business questions such as:

- How many claims were submitted by month?
- What is the total paid amount by provider?
- Which claims are denied, approved, or pending?
- What is the rejection rate across claims?
- How do member and provider trends change over time?

This project aims to solve that by creating a structured lakehouse pipeline from raw input to analytics-ready output.

---

## Architecture

The project follows the **Medallion Architecture**:

### Bronze Layer
Raw ingestion layer where source files are loaded as-is with ingestion metadata for traceability and auditing.

### Silver Layer
Cleaned and standardized layer where data quality checks, type casting, validation, and deduplication are applied.

### Gold Layer
Business-ready layer where curated fact and dimension-style datasets are prepared for analytics and reporting.

---

## Tech Stack

- **PySpark** – distributed data processing
- **Delta Lake** – ACID transactions and reliable lakehouse storage
- **Pandas** – lightweight data handling where required
- **Faker** – synthetic healthcare data generation
- **PyArrow** – columnar file support
- **YAML Config** – configurable paths and settings
- **pytest** – testing framework for validation and quality checks

---

## Core Data Domains

This project is designed around common healthcare data entities such as:

- **Claims**
- **Members**
- **Providers**
- **Payments**

These datasets are used to simulate realistic ingestion and transformation scenarios in a healthcare analytics platform.

---

## Project Structure

```text
healthcare_claims_lakehouse/
│
├── config/                  # YAML configuration files
├── data/
│   ├── landing/             # Raw source files
│   ├── bronze/              # Bronze Delta tables
│   ├── silver/              # Silver curated datasets
│   └── gold/                # Gold analytics-ready marts
│
├── notebooks/               # Project execution scripts / notebooks
├── src/                     # Reusable source code and utilities
├── tests/                   # Unit and validation tests
├── docs/                    # Project documentation
├── requirements.txt         # Python dependencies
└── README.md                # Project documentation

```
## Key Features

- Local lakehouse implementation using **PySpark** and **Delta Lake**
- **Bronze, Silver, and Gold** layered architecture
- Synthetic data generation for healthcare claims use cases
- Support for multiple source file formats such as CSV, JSON, and Parquet
- Metadata-driven raw ingestion for traceability and auditing
- Cleaned and standardized transformation layer for downstream consumption
- Business-ready curated datasets for analytics and reporting
- Testability using **pytest**
- Configurable project setup using **YAML**

---

## Current Implementation Status

### Implemented

- Spark and Delta Lake environment setup
- Synthetic healthcare data generation framework
- Foundational project structure for lakehouse development

### In Progress

- Bronze ingestion pipeline
- Silver transformation and data quality validation
- Gold dimensional and analytical marts
- Automated testing and validation coverage
- Documentation and project execution workflow

---

## Sample Pipeline Flow

1. Generate synthetic healthcare source data  
2. Load raw files into the Bronze layer with ingestion metadata  
3. Apply standardization, validation, and cleansing in the Silver layer  
4. Build analytics-ready curated datasets in the Gold layer  
5. Use the curated output for reporting, dashboarding, and architecture discussions  

---

## Why This Project

This project is designed to strengthen practical understanding of:

- Data lakehouse design
- PySpark transformations
- Delta Lake fundamentals
- Healthcare claims domain modeling
- Data quality and validation workflows
- Enterprise-style project structuring

It is also intended to serve as a strong portfolio project for **Data Engineer** roles.

---

### Getting Started

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd healthcare_claims_lakehouse
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv
.venv\Scripts\activate
```
### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the smoke test
```bash
python notebooks/00_smoke_test.py
```

### 5. Generate sample healthcare data
```bash
python notebooks/01_data_generator.py
```
---

### Future Enhancements
- Incremental load handling
- CDC processing
- Schema evolution support
- Data quality reporting dashboard
- Orchestration using Airflow or Control-M style scheduling
- Cloud version using AWS or Databricks

---

### Disclaimer

This project uses synthetic data only and is intended for learning, portfolio building, and data engineering practice.
No real patient or healthcare data is used.
