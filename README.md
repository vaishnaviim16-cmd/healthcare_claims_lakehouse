# Healthcare Claims Lakehouse

A Databricks-style local data engineering project built using PySpark and Delta Lake.

## Objective
Build a healthcare claims lakehouse using Bronze, Silver, and Gold architecture.

## Tech stack
- PySpark
- Delta Lake
- Pandas
- Faker
- PyArrow
- YAML config
- pytest

## Layers
- Bronze: raw ingestion with metadata
- Silver: cleaned and standardized datasets
- Gold: analytics-ready marts

## Current status
- Day 1: environment and Spark/Delta setup
- Day 2: synthetic data generation