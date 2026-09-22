# Modern Data Engineering ETL Pipeline

A practical end-to-end Data Engineering project demonstrating data ingestion, validation, incremental processing, change detection, Parquet-based storage, SQLite audit logging, Power BI reporting, pipeline automation, and local AI-powered shipment intelligence.

## Architecture

CSV Source
    ↓
Python + PySpark ETL
    ↓
Data Validation & Cleaning
    ↓
Incremental Processing
    ↓
Parquet
    ↓
Power BI Business Dashboard

                    └──→ SQLite Audit Database
                              ↓
                         Power BI Audit Data

Parquet Metrics
    ↓
AI-ready JSON
    ↓
Ollama + Qwen3
    ↓
Shipment Intelligence Report

## Project Features

- CSV-based shipment data ingestion
- Data validation and separation of valid/invalid records
- PySpark DataFrame processing
- Incremental load processing
- Insert / Update / Unchanged detection
- Column-level change history
- Versioned Parquet output
- SQLite-based ETL audit logging
- ETL execution tracking
- Processing duration tracking
- Local AI analysis using Ollama
- AI-generated shipment intelligence report
- Power BI business dashboard
- Windows Task Scheduler automation
- Windows batch execution

## Incremental Processing

The pipeline identifies records using ShipmentID.

For an existing shipment:

- New ShipmentID → INSERT
- Existing ShipmentID with changed business data → UPDATE
- Existing ShipmentID with no business changes → UNCHANGED

Change detection is performed against the business/source columns rather than generated processing columns.

## Data Validation

The pipeline validates shipment records before loading them into the target layer.

Invalid records are separated from the valid processing flow so that bad source data does not silently enter the business dataset.

## Target Storage

Processed business data is stored as Parquet.

The project uses versioned output directories to support repeated ETL runs and avoid problems caused by directly replacing Spark output directories on Windows.

The active target is tracked through:

data/output/latest_target.txt

Generated Parquet files are intentionally excluded from GitHub.

## Audit Logging

ETL execution information is stored in SQLite.

The audit layer tracks:

- Run ID
- Start time
- End time
- Status
- Inserted records
- Updated records
- Unchanged records
- Valid records
- Invalid records
- Processing duration
- Error information

Column-level changes are recorded in etl_change_history.

The SQLite audit database is intentionally excluded from GitHub because it is generated runtime data.

## AI Shipment Intelligence

The project includes a local AI layer using:

- Ollama
- Qwen3 4B
- Python
- JSON metrics

The ETL pipeline generates aggregated shipment metrics which are passed to the local LLM.

The AI produces a shipment intelligence report containing:

- Executive Summary
- Key Findings
- Potential Anomalies
- Operational Attention Areas
- Recommended Actions

No paid AI API or subscription is required.

## Power BI

Power BI is used for the business reporting layer.

The business data is sourced from the processed shipment dataset, while ETL audit information is available from SQLite through the SQLite ODBC driver.

The project keeps business reporting and operational audit information logically separated.

## Automation

The ETL can be executed through:

run_etl.bat

The project was also configured for Windows Task Scheduler automation.

This allows the ETL process to run without manually opening the Python script.

## Project Structure

modern-data-engineering-etl/
│
├── README.md
├── .gitignore
├── run_etl.bat
│
├── data/
│   └── raw/
│       └── shipments.csv
│
├── examples/
│   └── shipment_intelligence_example.md
│
└── src/
    ├── day3_dataframe.py
    └── ai_shipment_intelligence.py

Generated runtime data such as Parquet files, SQLite databases, logs, virtual environments, and generated AI files are excluded from version control.

## Technologies

- Python
- PySpark
- Apache Spark
- Parquet
- SQLite
- ODBC
- Power BI
- Ollama
- Qwen3
- Windows Task Scheduler
- PowerShell
- Git
- GitHub

## Running the ETL

After setting up the Python environment and required dependencies:

python src/day3_dataframe.py

The project also provides:

.\run_etl.bat

## Running the AI Layer

After the ETL has produced the current target data:

python src/ai_shipment_intelligence.py

The AI layer reads the active Parquet target, generates shipment metrics, sends the metrics to the local Ollama model, and produces the shipment intelligence report.

## Portfolio Focus

This project demonstrates practical Data Engineering concepts including:

- ETL pipeline development
- Batch processing
- Incremental data processing
- Data quality validation
- Change detection
- Auditability
- Distributed data processing with PySpark
- Columnar storage with Parquet
- Operational monitoring
- Business intelligence
- Local AI integration
- Pipeline automation

## Author

Shaista Sultana