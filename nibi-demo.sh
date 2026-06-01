#!/bin/bash

module load StdEnv/2023
module load cudacore/.12.2.2
module load java/17.0.6
module load spark/3.5.6
module load python/3.10

virtualenv -p python3.10 venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .

START_DATE="2026-04-16"
END_DATE="2026-04-21"
YEAR_MONTH="${START_DATE:0:7}"

# Download URBS Data
echo "Downloading URBS data..."
python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

# Uncompress URBS Data
echo "Decompressing URBS data..."
python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

# Execute trusting processor
echo "Processing trusting data..."
python dataprocessing/job/trust_ingestion.py -d "$YEAR_MONTH"

# Execute refined processor
echo "Processing refined data..."
python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j line
python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j itinerary
python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking

echo "All tasks completed!"
