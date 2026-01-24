# docker-workshop

This repository is a Docker & Data Engineering workshop project.
It demonstrates how to build a local data pipeline using Docker, Python, PostgreSQL, and how to provision cloud resources on GCP using Terraform.

The project is intentionally structured to resemble a real-world data pipeline:
- ingesting data into PostgreSQL
- running a local data pipeline with Docker
- provisioning basic GCP resources with Terraform

## Structure

    .
    ├── pipeline/    # Local data pipeline (Docker + Python + Postgres)
    └── terraform/   # GCP infrastructure (Terraform)


---

## Pipeline

The `pipeline/` directory implements a simple data ingestion pipeline.

A Python script (`ingest_data.py`) downloads NYC Yellow Taxi trip data
from a public GitHub release, processes it in chunks, and loads it into
a PostgreSQL database.

### Data Ingestion

The ingestion script:

- downloads compressed CSV files from DataTalksClub
- parses timestamps and enforces column dtypes
- creates the target table schema automatically
- inserts data into PostgreSQL in chunks

The script is configurable via command-line options.

Example:

```bash
python ingest_data.py \
  --user root \
  --password root \
  --host localhost \
  --port 5432 \
  --db ny_taxi \
  --table yellow_taxi_data \
  --year 2021 \
  --month 1
```

## Database
PostgreSQL and pgAdmin are started using Docker Compose:

```bash
cd pipeline
docker compose up
