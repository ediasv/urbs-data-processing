# URBS DATA PROCESSING

This pipeline downloads URBS open data, decompresses it, and produces trusted/refined Parquet datasets locally. The refined tracking job includes the interpolation step.

## Requirements
- Python 3.8+
- Java 8+ (required by PySpark)

## Install
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run with Podman (container)
```
podman build -t urbs-data-processing .
podman run --rm -it -v "$PWD/data:/app/data" -e DATA_DIR=/app/data urbs-data-processing \
  python dataprocessing/job/download_files.py -s 2022-07-11 -e 2022-07-11 -fd veiculos -fl veiculos.json.xz

podman run --rm -it -v "$PWD/data:/app/data" -e DATA_DIR=/app/data urbs-data-processing \
  python dataprocessing/job/decompress_files.py -s 2022-07-11 -e 2022-07-11 -fd veiculos -fl veiculos.json.xz

podman run --rm -it -v "$PWD/data:/app/data" -e DATA_DIR=/app/data urbs-data-processing \
  python dataprocessing/job/trust_ingestion.py -d 2022-07

podman run --rm -it -v "$PWD/data:/app/data" -e DATA_DIR=/app/data urbs-data-processing \
  python dataprocessing/job/refined_ingestion.py -ds 2022-07-11 -de 2022-07-11 -j tracking
```

## Data directory
All files are stored locally under a single data root. By default this is `./data`.

```
export DATA_DIR=./data  # optional, defaults to ./data
```

Outputs:
- `DATA_DIR/staging` (downloaded `.xz`)
- `DATA_DIR/raw` (decompressed JSON)
- `DATA_DIR/trusted` (Parquet)
- `DATA_DIR/refined` (Parquet, includes interpolation output)

## Download URBS data
```
python3 dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-16" -fd linhas -fl linhas.json.xz
python3 dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-16" -fd pontoslinha -fl pontosLinha.json.xz
python3 dataprocessing/job/download_files.py -s "2022-07-11" -e "2022-07-16" -fd veiculos -fl veiculos.json.xz
```

Parameters:
- `-fd`: `linhas`, `pontoslinha`, `veiculos`
- `-fl`: `linhas.json.xz`, `pontosLinha.json.xz`, `veiculos.json.xz`

## Decompress URBS data
```
python3 dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-16" -fd linhas -fl linhas.json.xz
python3 dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-16" -fd pontoslinha -fl pontosLinha.json.xz
python3 dataprocessing/job/decompress_files.py -s "2022-07-11" -e "2022-07-16" -fd veiculos -fl veiculos.json.xz
```

## Execute trust processor
```
python3 dataprocessing/job/trust_ingestion.py -d "2022-07"
```

## Execute refined processor (includes interpolation)
```
python3 dataprocessing/job/refined_ingestion.py -ds "2022-07-11" -de "2022-07-16" -j line
python3 dataprocessing/job/refined_ingestion.py -ds "2022-07-11" -de "2022-07-16" -j itinerary
python3 dataprocessing/job/refined_ingestion.py -ds "2022-07-11" -de "2022-07-16" -j tracking
```

`-j` options: `line`, `itinerary`, `tracking`

## Optional: run the full flow
```
./demo.sh
```
