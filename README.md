# Banking Transaction Fraud Analysis

This project builds a PySpark pipeline to identify potentially fraudulent banking transactions from raw transaction, customer, and merchant data.

## Project overview

The workflow includes:
- loading raw CSV files from the Data/raw folder
- cleaning and standardizing the datasets
- applying fraud-detection rules such as:
  - high-value transaction detection
  - burst of transactions in a short period
  - spending spikes compared with customer average
  - excessive daily transaction frequency
- printing a fraud report to the console

## Project structure

- src/main.py: entry point for the full pipeline
- src/ingest.py: loads and cleans the raw datasets
- src/fraud_rules.py: contains the fraud detection logic
- src/reports.py: prints summaries and sample flagged transactions
- Data/raw/: contains the input CSV files

## Prerequisites

Before running the project, make sure you have:
- Python 3.8+
- Java JDK installed and available on your PATH
- PySpark installed in the active Python environment

## Setup

Create and activate a virtual environment, then install the dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install pyspark
```

## Run the application

From the project root, run:

```bash
python src/main.py
```

If you are using the project virtual environment, the command is:

```bash
.venv\Scripts\python.exe src/main.py
```

## Expected output

The program will print:
- dataset counts for transactions, customers, and merchants
- fraud detection summary
- a sample of flagged transactions with the fraud reason
