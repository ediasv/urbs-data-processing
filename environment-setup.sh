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
