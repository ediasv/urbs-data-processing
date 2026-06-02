#!/bin/bash
#SBATCH --account=def-sukhjit-ab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=01:00:00
#SBATCH --array=1-114
#SBATCH --output=logs/urbs_pipeline_%A_%a.out

mkdir -p logs

# Clear environment to avoid inheriting conflicting shell variables
module purge

# Load required modules
module load StdEnv/2023
module load java/17.0.6
module load spark/3.5.6
module load python/3.10

source venv/bin/activate

# Calculate dates based on the array task ID
# 2017-01 to 2026-06 is exactly 114 months
OFFSET=$((114 - 1))

# Map the offset to the first day of the target month
export START_DATE=$(date -d "2017-01-01 + ${OFFSET} months" +%Y-%m-01)

# Add one month and subtract one day to get the exact last day of that month
export END_DATE=$(date -d "${START_DATE} + 1 month - 1 day" +%Y-%m-%d)
export YEAR_MONTH="${START_DATE:0:7}"

echo "Processing URBS tracking data for: $YEAR_MONTH (from $START_DATE to $END_DATE)"

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
    --master local[*] \
    --driver-memory 14G \
    dataprocessing/job/trust_ingestion.py -d "$YEAR_MONTH"

# Execute refined processor: Dimensions
echo "Processing refined dimension data on CPU..."
spark-submit \
    --master local[*] \
    --driver-memory 14G \
    dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j line

spark-submit \
    --master local[*] \
    --driver-memory 14G \
    dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j itinerary

echo "Processing refined tracking data on CPU..."
time spark-submit \
    --master local[*] \
    --driver-memory 14G \
    dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking
