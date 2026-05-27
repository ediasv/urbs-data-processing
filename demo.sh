#!/bin/bash

START_DATE="2022-07-11"
END_DATE="2022-07-11"

# Build container environment (Podman-aware)
echo "Building container image(s)..."
# Prefer podman if available, otherwise fall back to docker
if command -v podman >/dev/null 2>&1; then
  CONTAINER_CMD="podman compose"
elif command -v docker >/dev/null 2>&1; then
  CONTAINER_CMD="docker compose"
else
  echo "Neither podman nor docker was found in PATH. Install one to proceed." >&2
  exit 1
fi

echo "Using: $CONTAINER_CMD"
$CONTAINER_CMD up --build -d

# Initialize MySQL schema from the sibling repository
echo "Initializing MySQL schema..."
$CONTAINER_CMD exec mysql mysql -uroot -p123456789 -e "DROP DATABASE IF EXISTS busanalysis_dw; DROP DATABASE IF EXISTS busanalysis_etl;"
$CONTAINER_CMD exec -T mysql mysql -uroot -p123456789 < init-scripts/create_busanalysis_etl.sql
$CONTAINER_CMD exec -T mysql mysql -uroot -p123456789 < init-scripts/create_busanalysis_dw.sql

# Download URBS Data
echo "Downloading URBS data..."
$CONTAINER_CMD exec jupyterlab python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
$CONTAINER_CMD exec jupyterlab python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
$CONTAINER_CMD exec jupyterlab python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

# Uncompress URBS Data
echo "Decompressing URBS data..."
$CONTAINER_CMD exec jupyterlab python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
$CONTAINER_CMD exec jupyterlab python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
$CONTAINER_CMD exec jupyterlab python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

# Execute trusting processor
echo "Processing trusting data..."
$CONTAINER_CMD exec jupyterlab python dataprocessing/job/trust_ingestion.py -d "2022-07"

# Execute refined processor
echo "Processing refined data..."
$CONTAINER_CMD exec jupyterlab python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j line
$CONTAINER_CMD exec jupyterlab python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j itinerary
$CONTAINER_CMD exec jupyterlab python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking

# Load data into MySQL
echo "Loading data into MySQL..."
$CONTAINER_CMD exec jupyterlab python dataprocessing/job/mysql_loader.py -ds "$START_DATE" -de "$END_DATE"

echo "All tasks completed!"
