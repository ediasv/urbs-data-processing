from argparse import ArgumentParser
from pathlib import Path

import pandas as pd


def discover_parquet_files(source_path: Path) -> list[Path]:
    if source_path.is_file():
        return [source_path]

    if not source_path.exists():
        raise FileNotFoundError(source_path)

    parquet_files = [
        path
        for path in source_path.rglob("*")
        if path.is_file()
        and not path.name.startswith((".", "_"))
        and path.name.endswith(".parquet")
    ]

    if not parquet_files:
        raise ValueError(f"No parquet files found under {source_path}")

    return sorted(
        parquet_files, key=lambda path: path.relative_to(source_path).as_posix()
    )


def main() -> int:
    parser = ArgumentParser(
        description="Flatten a parquet directory tree into a single parquet file"
    )
    parser.add_argument("source", help="Source parquet file or directory")
    parser.add_argument("target", help="Target single parquet file")
    args = parser.parse_args()

    source_path = Path(args.source)
    target_path = Path(args.target)

    parquet_files = discover_parquet_files(source_path)
    print(f"SOURCE: {source_path}")
    print(f"TARGET: {target_path}")
    print(f"PART FILES: {len(parquet_files)}")

    frames = [pd.read_parquet(path) for path in parquet_files]
    if not frames:
        raise ValueError(f"No readable parquet files found under {source_path}")

    merged = pd.concat(frames, ignore_index=True)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(target_path, index=False)

    print(f"ROWS: {len(merged)}")
    print("RESULT: WROTE SINGLE PARQUET FILE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
