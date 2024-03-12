# HDB Resale Ingestion Pipelines

## Introduction
Used to combine HDB Resale datasets obtained via data.gov.sg

The workflow contains a two steps:
1. Ingestion to a datamart
2. Generating entities

## Quickstart
Initialise your Python environment. I use `pyenv` / `virtualenv`, and Python `3.11.1
```
pyenv virtualenv 3.11.1 env
pyenv activate env
pip install -r requirements.txt
```

In the `assets/datasets/downloaded` folder, download the HDB datasets from data.gov.sg (Use `source.md` to help obtain the link to download)

Set the `PYTHONPATH` and go to the `src` folder
```
export PYTHONPATH="/path/to/hdb-resale-transactions-ingestion-pipeline/src"
cd /path/to/hdb-resale-transactions-ingestion-pipeline/src
```

Run `pipelines/datamart/ingest_to_datamart.py` and `pipelines/entities/generate_entities.py`
```
python pipelines/datamart/ingest_to_datamart.py
python pipelines/entities/generate_entities.py 
```
