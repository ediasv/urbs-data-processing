#!/bin/bash

START_DATE="2022-07-11"
END_DATE="2022-07-11"
DATA_DIR="${DATA_DIR:-data}"
MONTH="${START_DATE:0:7}"

export DATA_DIR

# Download URBS Data
echo "Downloading URBS data..."
python3 dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
python3 dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
python3 dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

# Uncompress URBS Data
echo "Decompressing URBS data..."
python3 dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
python3 dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
python3 dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

# Execute trusting processor
echo "Processing trusting data..."
python3 dataprocessing/job/trust_ingestion.py -d "$MONTH"

# Execute refined processor
echo "Processing refined data..."
python3 dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j line
python3 dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j itinerary
python3 dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking

echo "All tasks completed!"
