# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Starting Services
```bash
# Batch processing with MinIO and PostgreSQL
docker compose -f datalake-docker-compose.yaml up -d

# Stream processing with Kafka and Debezium
docker compose -f kafka-debezium-docker-compose.yaml up -d

# Airflow for ML pipeline orchestration
docker compose -f airflow-docker-compose.yaml up -d

# Monitoring stack (Prometheus + Grafana)
cd monitoring/
docker compose -f prom-graf-docker-compose.yaml up -d

# ELK stack for log monitoring
cd monitoring/elk/
docker compose -f elk-docker-compose.yml -f extensions/filebeat/filebeat-compose.yml up -d
```

### Data Pipeline Operations
```bash
# Initialize database schema and tables
python utils/create_schema.py
python utils/create_table.py

# Upload raw data to Delta Lake in MinIO
python utils/write_delta_table.py
python utils/upload_data_to_datalake.py

# Run batch processing (Spark-based tokenization and staging)
python batch_processing/main.py

# Run stream processing (Flink-based real-time processing)
python stream_processing/main.py

# Stream data to PostgreSQL for CDC
python utils/streaming_data_to_postgresql.py
```

### Data Transformation and Quality
```bash
# dbt data transformation
cd data_transformation/
dbt clean
dbt deps
dbt build --target prod

# Data validation with Great Expectations
# Use notebooks: data_validation/full_flow.ipynb and data_validation/reload_and_validate.ipynb
```

### Machine Learning Workflow
```bash
# DVC pipeline management
dvc repro                    # Run the full ML pipeline
dvc push                     # Push data/models to MinIO remote
dvc remote modify minio_remote endpointurl http://localhost:9000  # For local development
dvc remote modify minio_remote endpointurl http://minio:9000      # For Docker containers

# Model training
python model_experiment/train.py
python model_experiment/extract_data.py
```

### Debezium CDC Setup
```bash
cd debezium/
bash run.sh register_connector configs/postgresql-cdc.json
```

## Architecture Overview

This is an end-to-end MLOps pipeline for toxic comment classification with:

### Data Flow
1. **Raw Data**: CSV files → MinIO object storage (Delta Lake format)
2. **Batch Processing**: Spark reads from MinIO, tokenizes text using DistilBERT, loads to PostgreSQL staging
3. **Stream Processing**: Debezium captures PostgreSQL changes → Kafka → Flink processes → PostgreSQL staging
4. **Data Transformation**: dbt transforms staging data to production schema
5. **ML Pipeline**: DVC orchestrates data extraction and model training with MLflow tracking

### Key Components
- **MinIO**: S3-compatible object storage for Delta Lake
- **PostgreSQL**: Data warehouse with staging/production schemas
- **Apache Spark**: Batch processing for text tokenization
- **Apache Flink**: Real-time stream processing
- **Kafka + Debezium**: Change Data Capture and streaming
- **dbt**: Data transformation and modeling
- **DVC**: Data versioning and ML pipeline orchestration
- **MLflow**: Model tracking and versioning
- **Great Expectations**: Data quality validation
- **Airflow**: Workflow orchestration

### Configuration
- Main config: `configs/config.yaml` (MinIO, Spark, PostgreSQL settings)
- ML config: `model_experiment/config.py` (model parameters, paths)
- DVC config: `dvc.yaml` (pipeline stages)
- dbt config: `data_transformation/dbt_project.yml`

### Model Architecture
- Base model: DistilBERT (`distilbert-base-uncased`)
- Text preprocessing: Max length 512 tokens
- Binary classification for toxic comment detection
- Threshold: 0.45 for toxicity classification

### Development Notes
- Each component has its own `requirements.txt`
- Jars for Flink connectors are in `/jars/` directory
- Use absolute paths when referencing project files
- PostgreSQL connection details are in `configs/config.yaml`
- Great Expectations expects PostgreSQL staging schema for validation

