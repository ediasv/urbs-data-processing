#!/bin/bash

START_DATE="2026-04-16"
END_DATE="2026-04-30"
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
    --master local[2] \
    --executor-memory 8G \
    --driver-memory 4G \
    --jars rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.driver.extraClassPath=rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.executor.extraClassPath=rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.explain=ALL \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.task.resource.gpu.amount=0.5 \
    --conf spark.rapids.memory.gpu.allocFraction=0.4 \
    --conf spark.rapids.memory.gpu.pooling=ARENA \
    dataprocessing/job/trust_ingestion.py -d "$YEAR_MONTH"

# Execute refined processor
echo "Processing refined data..."
python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j line
python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j itinerary
python dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking

spark-submit \
    --master local[2] \
    --executor-memory 8G \
    --driver-memory 4G \
    --jars rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.driver.extraClassPath=rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.executor.extraClassPath=rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.explain=ALL \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.task.resource.gpu.amount=0.5 \
    --conf spark.rapids.memory.gpu.allocFraction=0.4 \
    --conf spark.rapids.memory.gpu.pooling=ARENA \
    dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j line

spark-submit \
    --master local[2] \
    --executor-memory 8G \
    --driver-memory 4G \
    --jars rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.driver.extraClassPath=rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.executor.extraClassPath=rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.explain=ALL \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.task.resource.gpu.amount=0.5 \
    --conf spark.rapids.memory.gpu.allocFraction=0.4 \
    --conf spark.rapids.memory.gpu.pooling=ARENA \
    dataprocessing/job/refined_ingestion.py ds "$START_DATE" -de "$END_DATE" -j itinerary

spark-submit \
    --master local[2] \
    --executor-memory 8G \
    --driver-memory 4G \
    --jars rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.driver.extraClassPath=rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.executor.extraClassPath=rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.explain=ALL \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.task.resource.gpu.amount=0.5 \
    --conf spark.rapids.memory.gpu.allocFraction=0.4 \
    --conf spark.rapids.memory.gpu.pooling=ARENA \
    dataprocessing/job/refined_ingestion.py ds "$START_DATE" -de "$END_DATE" -j tracking

echo "All tasks completed!"
