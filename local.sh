#!/bin/bash

unset JAVA_TOOL_OPTIONS

echo "Activating virtualenv..."
source venv/bin/activate

# Calculate dates based on the array task ID
# 2017-01 to 2026-06 is exactly 114 months

START_DATE="2017-01-25"
END_DATE="2017-01-30"
YEAR_MONTH="${START_DATE:0:7}"
YEAR="${START_DATE:0:4}"
MONTH="${START_DATE:5:2}"

export START_DATE
export END_DATE
export YEAR_MONTH
export YEAR
export MONTH

echo "Processing URBS tracking data for: $YEAR_MONTH (from $START_DATE to $END_DATE)"

echo "Downloading URBS data..."
echo "  - linhas"
python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
echo "  - pontoslinha"
python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
echo "  - veiculos"
python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

echo "Decompressing URBS data..."
echo "  - linhas"
python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
echo "  - pontoslinha"
python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
echo "  - veiculos"
python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

echo "Processing trusting data..."
echo "  - trust_ingestion"
spark-submit \
  --master local[*] \
  --driver-memory 14G \
  dataprocessing/job/trust_ingestion.py -d "$YEAR_MONTH"

echo "Processing refined ingestion: line"
echo "  - refined_ingestion line"
spark-submit \
  --master local[*] \
  --driver-memory 14G \
  dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j line

echo "Processing refined ingestion: itinerary"
echo "  - refined_ingestion itinerary"
spark-submit \
  --master local[*] \
  --driver-memory 14G \
  dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j itinerary

echo "Processing refined ingestion: tracking"
echo "  - refined_ingestion tracking"
time spark-submit \
  --master local[*] \
  --driver-memory 14G \
  dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking
