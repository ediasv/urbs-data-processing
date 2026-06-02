#!/bin/bash
#SBATCH --account=def-sukhjit-ab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=12
#SBATCH --mem=24G
#SBATCH --time=01:30:00
#SBATCH --array=1-114
#SBATCH --output=~/logs/urbs_pipeline_%A_%a.out

: "${SLURM_ARRAY_TASK_ID:?SLURM_ARRAY_TASK_ID is not set}"

if (( SLURM_ARRAY_TASK_ID < 1 || SLURM_ARRAY_TASK_ID > 114 )); then
  echo "Invalid SLURM_ARRAY_TASK_ID=${SLURM_ARRAY_TASK_ID} (expected 1-114)" >&2
  exit 1
fi

cd ~/repos/urbs-data-processing

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
OFFSET=$((SLURM_ARRAY_TASK_ID - 1))

# Map the offset to the first day of the target month
export START_DATE=$(date -d "2017-01-01 + ${OFFSET} months" +%Y-%m-01)

# Add one month and subtract one day to get the exact last day of that month
export END_DATE=$(date -d "${START_DATE} + 1 month - 1 day" +%Y-%m-%d)
export YEAR_MONTH="${START_DATE:0:7}"
export YEAR="${START_DATE:0:4}"
export MONTH="${START_DATE:5:2}"

echo "Processing URBS tracking data for: $YEAR_MONTH (from $START_DATE to $END_DATE)"

echo "Downloading URBS data..."
python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
python dataprocessing/job/download_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

echo "Decompressing URBS data..."
python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd linhas -fl linhas.json.xz
python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd pontoslinha -fl pontosLinha.json.xz
python dataprocessing/job/decompress_files.py -s "$START_DATE" -e "$END_DATE" -fd veiculos -fl veiculos.json.xz

echo "Processing trusting data..."
spark-submit \
    --master local[*] \
    --driver-memory 14G \
    dataprocessing/job/trust_ingestion.py -d "$YEAR_MONTH"

echo "Processing refined ingestion: line"
spark-submit \
    --master local[*] \
    --driver-memory 14G \
    dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j line

echo "Processing refined ingestion: itinerary"
spark-submit \
    --master local[*] \
    --driver-memory 14G \
    dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j itinerary

echo "Processing refined ingestion: tracking"
time spark-submit \
    --master local[*] \
    --driver-memory 14G \
    dataprocessing/job/refined_ingestion.py -ds "$START_DATE" -de "$END_DATE" -j tracking

echo "Uploading to Hugging Face..."
python dataprocessing/job/upload_huggingface.py -d "$YEAR_MONTH"

# erase files from the month
echo "Erasing files from the month..."
rm -rf data/raw/$YEAR_MONTH
rm -rf data/staging/$YEAR_MONTH
rm -rf data/trusted/busstops/"year=${YEAR}"/"month=${MONTH}"
rm -rf data/trusted/lines/"year=${YEAR}"/"month=${MONTH}"
rm -rf data/trusted/vehicles/"year=${YEAR}"/"month=${MONTH}"
rm -rf data/refined/bus_itineraries/"year=${YEAR}"/"month=${MONTH}"
rm -rf data/refined/bus_tracking/"year=${YEAR}"/"month=${MONTH}"
rm -rf data/refined/bus_lines/"year=${YEAR}"/"month=${MONTH}"
