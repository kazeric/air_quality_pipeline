# air_quality_pipeline

# Air Quality Pipeline

This repository contains an air-quality data ingestion pipeline used for collecting, batching, and consuming sensor data. It includes scripts for streaming and batch ingestion, a Kafka consumer , and dashboard/query artifacts for Grafana. The project is intended as a data engineering exercise and a reference for setting up ingestion pipelines and dashboards for time-series data.

<img width="1980" height="360" alt="DFA for a drink dispenser" src="https://github.com/user-attachments/assets/0892f741-3625-44c5-828a-e7486828e9fd" />


<img width="1620" height="883" alt="image" src="https://github.com/user-attachments/assets/a5c18f0b-bf44-4521-9c94-3f827bdd18d6" />

## Repository structure

- `data_ingestion.py` - Script to ingest a single batch or sample of air-quality data. Use this for simple runs or testing.
- `batch_data_igestion.py` - Script to run batch ingestion (note: filename contains a small typo `igestion` in the repo). Use for scheduled or larger imports.
- `kafka_consumer.py` - Kafka consumer script that reads messages from a topic and processes/stores them. 
- `test.ipynb` - Jupyter notebook with exploratory analysis or demos.
- `Confluence_setup/` - Supporting documents or templates for Confluence (project documentation).
- `Grafana_Dashboard/` - Grafana dashboard JSON and SQL query examples used to visualize air-quality metrics.

Files inside `Grafana_Dashboard/`:
- `Air quality-1760993250840.json` - Grafana dashboard export (JSON).
- `grafana_queries.sql` - Example SQL queries used by the dashboard panels.

## Quick start

These steps assume you have Python 3.8+ installed. You can run the scripts locally for development and testing.

1. Create and activate a virtual environment (recommended):

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install required packages :

```bash
pip install --upgrade pip
# Example packages commonly used by ingestion pipelines. Add or replace with your project's requirements.
pip install kafka-python pandas requests
```

3. Run a single ingestion:

```bash
python data_ingestion.py
```

4. Run batch ingestion:

```bash
python batch_data_igestion.py
```

5. Run the Kafka consumer (make sure Kafka is running and environment variables or config inside `kafka_consumer.py` are set):

```bash
python kafka_consumer.py
```

Note: The scripts may expect specific environment variables or configuration (Kafka brokers, topic names, database connection strings). Inspect the top of each script to find configuration keys and set them before running.

## Configuration

- Kafka: configure bootstrap servers and topic names in `kafta_consumer.py` or via environment variables (edit the script as needed).
- Storage: if the consumer writes to a database or files, update connection strings/paths in the corresponding script.

## Grafana dashboard and queries

The `Grafana_Dashboard` folder contains an exported dashboard JSON and example SQL queries. To import the dashboard into Grafana:

1. Open Grafana > Dashboards > Import
2. Upload `Air quality-1760993250840.json` or paste its contents
3. Configure the data source and any required variables

Use `grafana_queries.sql` as a starting point for your data source queries.



## Next steps / Improvements

- Create `requirements.txt` and document exact versions.
- Add a `docker-compose.yml` to run local Kafka and Zookeeper for integration testing.
- Add logging, retries, and error handling to ingestion and consumer scripts.


