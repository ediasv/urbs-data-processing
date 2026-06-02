#!/bin/bash

# salloc --time=00:20:00 --gpus-per-node=h100_1g.10gb:1 --ntasks=1 --cpus-per-task=2 --mem=20G --account=def-sukhjit-ab

module load StdEnv/2023
module load cudacore/.12.2.2
module load java/17.0.6
module load spark/3.5.6
module load python/3.10

source venv/bin/activate

export EVENT_LOG_DIR="./tmp/spark-events"
export START_DATE="2024-08-01"
export END_DATE="2024-08-31"
export YEAR_MONTH="${START_DATE:0:7}"
export EVENT_LOG_DIR=/tmp/spark-events

time spark-submit \
  --master local[2] \
  --executor-memory 8G \
  --driver-memory 8G \
  dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking

time spark-submit \
    --master local[2] \
    --executor-memory 8G \
    --driver-memory 8G \
    --jars rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.driver.extraClassPath=rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.executor.extraClassPath=rapids-4-spark_2.12-26.04.2.jar \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.incompatibleDateFormats.enabled=true \
    dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking
