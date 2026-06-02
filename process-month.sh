#!/bin/bash
#SBATCH --account=def-sukhjit-ab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=01:00:00
#SBATCH --array=1-126
#SBATCH --output=logs/urbs_pipeline_%A_%a.out

mkdir -p logs

# Clear environment to avoid inheriting conflicting shell variables
module purge

# Load required modules (cudacore removed as GPU is not optimal for this job)
module load StdEnv/2023
module load java/17.0.6
module load spark/3.5.6
module load python/3.10

source venv/bin/activate

# TODO:
# Run for every month since 2017-01 until 2026-06
export START_DATE="2020-05-01"
export END_DATE="2020-05-31"
export YEAR_MONTH="${START_DATE:0:7}"

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
