#!/bin/bash
#SBATCH --account=def-sukhjit-ab
#SBATCH --time=01:00:00
#SBATCH --gpus=h100:1
# #SBATCH --ntasks=1
#SBATCH --cpus-per-task=12
#SBATCH --mem=32G
#SBATCH --array=1-108
#SBATCH --output=logs/urbs_pipeline_%A_%a.out

mkdir -p logs

START_YEAR=2015
OFFSET=$((SLURM_ARRAY_TASK_ID - 1))
YEAR=$((START_YEAR + OFFSET / 12))
MONTH=$(( (OFFSET % 12) + 1 ))
YEAR_MONTH=$(printf "%04d-%02d" $YEAR $MONTH)

echo "Starting Nibi compute job for historical data: $YEAR_MONTH"

# Execute the pipeline
python3 main.py "$YEAR_MONTH"
