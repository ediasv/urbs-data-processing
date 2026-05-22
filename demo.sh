#!/bin/bash

START_DATE="2022-07-11"
END_DATE="2022-07-11"

# Build docker-image
echo "Building docker image..."
docker compose up --build -d

# Initialize MySQL schema from the sibling repository
echo "Initializing MySQL schema..."
docker compose exec mysql mysql -uroot -p123456789 -e "DROP DATABASE IF EXISTS busanalysis_dw; DROP DATABASE IF EXISTS busanalysis_etl;"
docker compose exec -T mysql mysql -uroot -p123456789 < init-scripts/create_busanalysis_etl.sql
docker compose exec -T mysql mysql -uroot -p123456789 < init-scripts/create_busanalysis_dw.sql

# Download URBS Data
echo "Downloading URBS data..."
docker compose exec jupyterlab python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
docker compose exec jupyterlab python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
docker compose exec jupyterlab python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

# Uncompress URBS Data
echo "Decompressing URBS data..."
docker compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
docker compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
docker compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

# Execute trusting processor
echo "Processing trusting data..."
docker compose exec jupyterlab python dataprocessing/job/trust_ingestion.py -d "2022-07"

# Execute refined processor
echo "Processing refined data..."
docker compose exec jupyterlab python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j line
docker compose exec jupyterlab python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j itinerary
docker compose exec jupyterlab python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking

# Load data into MySQL
echo "Loading data into MySQL..."
docker compose exec jupyterlab python dataprocessing/job/mysql_loader.py -ds "$START_DATE" -de "$END_DATE"

echo "All tasks completed!"
