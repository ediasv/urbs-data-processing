import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATA_DIR", "data")).resolve()


def data_path(*parts: str) -> Path:
    return DATA_DIR.joinpath(*parts)


def data_path_str(*parts: str) -> str:
    return str(data_path(*parts))
