#!/bin/bash
#SBATCH --account=def-sukhjit-ab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=12
#SBATCH --mem=32G
#SBATCH --time=01:30:00
#SBATCH --array=1-114%1
#SBATCH --output=/home/ediasv/logs/urbs_pipeline_%A_%a.out

mkdir -p /home/ediasv/logs

cd /home/ediasv/repos/urbs-data-processing || exit

# Keep the log file for stderr only; suppress normal progress output.
# exec 1>/dev/null

# Clear environment to avoid inheriting conflicting shell variables
echo "Clearing environment..."
module purge

# Load required modules
echo "Loading modules..."
module load StdEnv/2023
module load java/17.0.6
module load spark/3.5.6
module load python/3.10

unset JAVA_TOOL_OPTIONS

echo "Activating virtualenv..."
source venv/bin/activate

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

echo "Uploading to Hugging Face..."
echo "  - upload_huggingface"
python dataprocessing/job/upload_huggingface.py -d "$YEAR_MONTH"

# erase files from the month
echo "Erasing files from the month..."
rm -rf data/raw/"$YEAR_MONTH"
rm -rf data/staging/"$YEAR_MONTH"
rm -rf data/trusted/busstops/"year=${YEAR}"/"month=${MONTH}"
rm -rf data/trusted/lines/"year=${YEAR}"/"month=${MONTH}"
rm -rf data/trusted/vehicles/"year=${YEAR}"/"month=${MONTH}"
rm -rf data/refined/bus_itineraries/"year=${YEAR}"/"month=${MONTH}"
rm -rf data/refined/bus_tracking/"year=${YEAR}"/"month=${MONTH}"
rm -rf data/refined/bus_lines/"year=${YEAR}"/"month=${MONTH}"
