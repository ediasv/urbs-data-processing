#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path


def parse_year_month(value: str) -> tuple[int, int]:
    try:
        dt = datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Invalid date format. Expected yyyy-mm (e.g. 2026-04)."
        ) from exc
    return dt.year, dt.month


def month_date_range(year: int, month: int) -> tuple[str, str]:
    last_day = calendar.monthrange(year, month)[1]
    start_date = date(year, month, 1).isoformat()
    end_date = date(year, month, last_day).isoformat()
    return start_date, end_date


def run_step(label: str, command: list[str], cwd: Path) -> None:
    print(label)
    subprocess.run(command, check=True, cwd=cwd)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the full URBS data processing pipeline for a month."
    )
    parser.add_argument(
        "year_month",
        type=parse_year_month,
        help="Month to process in yyyy-mm format (e.g. 2026-04).",
    )
    args = parser.parse_args()

    year, month = args.year_month
    year_month = f"{year:04d}-{month:02d}"
    start_date, end_date = month_date_range(year, month)

    repo_root = Path(__file__).resolve().parent
    jobs_dir = repo_root / "dataprocessing" / "job"
    python = sys.executable

    downloads = [
        ("linhas", "linhas.json.xz"),
        ("pontoslinha", "pontosLinha.json.xz"),
        ("veiculos", "veiculos.json.xz"),
    ]

    for folder, filename in downloads:
        run_step(
            f"Downloading {folder}...",
            [
                python,
                str(jobs_dir / "download_files.py"),
                "-s",
                start_date,
                "-e",
                end_date,
                "-fd",
                folder,
                "-fl",
                filename,
            ],
            repo_root,
        )

    for folder, filename in downloads:
        run_step(
            f"Decompressing {folder}...",
            [
                python,
                str(jobs_dir / "decompress_files.py"),
                "-s",
                start_date,
                "-e",
                end_date,
                "-fd",
                folder,
                "-fl",
                filename,
            ],
            repo_root,
        )

    run_step(
        "Processing trusted data...",
        [python, str(jobs_dir / "trust_ingestion.py"), "-d", year_month],
        repo_root,
    )

    for job in ("line", "itinerary", "tracking"):
        run_step(
            f"Processing refined {job} data...",
            [
                python,
                str(jobs_dir / "refined_ingestion.py"),
                "-ds",
                start_date,
                "-de",
                end_date,
                "-j",
                job,
            ],
            repo_root,
        )

    run_step(
        "Uploading refined data to Hugging Face...",
        [python, str(jobs_dir / "upload_huggingface.py"), "-d", year_month],
        repo_root,
    )

    print("All tasks completed!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
