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

START_DATE="2026-03-01"
END_DATE="2026-03-30"
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
spark-submit \
    --master local[4] \
    --executor-memory 8G \
    --driver-memory 4G \
    dataprocessing/job/trust_ingestion.py -d "$YEAR_MONTH"

# Execute refined processor: Dimensions (CPU ONLY)
echo "Processing refined dimension data on CPU..."
spark-submit \
    --master local[4] \
    --executor-memory 8G \
    --driver-memory 4G \
    dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j line

spark-submit \
    --master local[4] \
    --executor-memory 8G \
    --driver-memory 4G \
    dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j itinerary

# Execute refined processor: Tracking (GPU ENABLED)
echo "Processing refined tracking data on GPU..."
spark-submit \
    --master local[4] \
    --executor-memory 6G \
    --driver-memory 6G \
    --jars rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.driver.extraClassPath=rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.executor.extraClassPath=rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.incompatibleDateFormats.enabled=true \
    dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking

echo "All tasks completed!"
