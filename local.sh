#!/bin/bash

source venv/bin/activate

START_DATE="2026-03-05"
END_DATE="2026-03-10"
YEAR_MONTH="${START_DATE:0:7}"

export START_DATE
export END_DATE
export YEAR_MONTH
export YEAR
export MONTH

python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

python dataprocessing/job/trust_ingestion.py -d "$YEAR_MONTH"

python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j line
python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j itinerary
python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking
