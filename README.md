# Customer 360 — PySpark Identity Resolution & Analytics Pipeline

A PySpark data pipeline that takes messy customer records from multiple sign-up
channels (web, mobile, store), merges duplicate records of the same person into a
single customer profile (identity resolution), and computes per-customer spend
analytics and segments.

```
customers.csv ─┐
               ├─► clean ─► resolve identities ─► join orders ─► metrics + segments ─► Parquet
orders.csv ────┘
```

**Sample run:** 332 raw records → 200 unique customers (132 duplicates merged).

## Features
- **Data cleaning:** normalizes inconsistent emails and phone formats
- **Identity resolution:** links duplicate records with Spark window functions
- **Analytics:** order count, total spend, last order date, and customer segment (VIP / Regular / Occasional / Inactive)
- **Output:** results written as Parquet
- **Tested:** unit tests for each pipeline stage with pytest

## Tech stack
Python · PySpark · pytest

## Run locally
Requires Python 3.9+ and Java 17 or 21.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python data/generate_data.py   # generate sample data
python src/pipeline.py         # run the pipeline
python -m pytest -v            # run tests
```

## Project structure
```
data/generate_data.py   sample data generator (with intentional duplicates)
src/pipeline.py         load → clean → resolve → analytics → write
tests/test_pipeline.py  unit tests for each stage
```

## Future work
- Match identities on phone number as well as email
- Read from and write to AWS S3
- Add a data-quality report step
