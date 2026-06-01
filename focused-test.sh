#!/bin/bash

module load StdEnv/2023
module load cudacore/.12.2.2
module load java/17.0.6
module load spark/3.5.6
module load python/3.10

source venv/bin/activate

START_DATE="2026-04-16"
END_DATE="2026-04-18"
YEAR_MONTH="${START_DATE:0:7}"

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
#    --conf spark.sql.shuffle.partitions=4 \
    dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking

echo "All tasks completed!"
