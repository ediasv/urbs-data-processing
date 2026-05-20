#!/bin/bash

# Build docker-image
echo "Building docker image..."
docker compose up --build -d

# Download URBS Data
echo "Downloading URBS data..."
docker compose exec jupyterlab python dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-17" -fd linhas -fl linhas.json.xz
docker compose exec jupyterlab python dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-17" -fd pontoslinha -fl pontosLinha.json.xz
docker compose exec jupyterlab python dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-17" -fd veiculos -fl veiculos.json.xz

# Uncompress URBS Data
echo "Decompressing URBS data..."
docker compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-17" -fd linhas -fl linhas.json.xz
docker compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-17" -fd pontoslinha -fl pontosLinha.json.xz
docker compose exec jupyterlab python dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-17" -fd veiculos -fl veiculos.json.xz

# Execute trusting processor
echo "Processing trusting data..."
docker compose exec jupyterlab python dataprocessing/job/trust_ingestion.py -d "2022-07"

# Execute refined processor
echo "Processing refined data..."
docker compose exec jupyterlab python dataprocessing/job/refined_ingestion.py -ds "2022-07-11" -de "2022-07-17" -j line
docker compose exec jupyterlab python dataprocessing/job/refined_ingestion.py -ds "2022-07-11" -de "2022-07-17" -j itinerary
docker compose exec jupyterlab python dataprocessing/job/refined_ingestion.py -ds "2022-07-11" -de "2022-07-17" -j tracking

# Load data into MySQL
echo "Loading data into MySQL..."
docker compose exec jupyterlab python dataprocessing/job/mysql_loader.py -ds "2022-07-11" -de "2022-07-17"

echo "All tasks completed!"
