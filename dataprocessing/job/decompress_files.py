import lzma
import os
import shutil
import sys
from argparse import ArgumentParser
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(".."))

from dataprocessing.config import data_path

parser = ArgumentParser()
parser.add_argument(
    "-s", "--start-date", dest="start_date", help="start date", metavar="DATE"
)
parser.add_argument(
    "-e", "--end-date", dest="end_date", help="end date", metavar="DATE"
)
parser.add_argument("-fd", "--folder", dest="folder", help="folder", metavar="FOLDER")
parser.add_argument("-fl", "--file", dest="file", help="file", metavar="FILE")

parser.add_argument(
    "-d", "--delete", dest="delete", help="delete staging files", metavar="DELETE"
)

args = parser.parse_args()

start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
folder = args.folder
file = args.file
delete = args.delete


def decompress_files(folder, file, start_date, end_date):
    print(f"Date range: {start_date} --> {end_date}")
    print(f"Filename: {file}")

    delta = end_date - start_date
    print(delta)

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        download_file_day = day.strftime("%Y_%m_%d")

        datareferencia = day.replace(day=1).strftime("%Y-%m")

        base_folder = data_path("raw", datareferencia, folder)
        base_folder.mkdir(parents=True, exist_ok=True)

        fstaging = data_path(
            "staging",
            datareferencia,
            folder,
            f"{download_file_day}_{file}",
        )
        fraw = base_folder / f"{download_file_day}_{file.replace('.xz', '')}"

        if not fstaging.exists():
            print(f"Missing staging file: {fstaging}")
            continue

        if fraw.exists():
            print(f"{fraw} already decompressed; skipping")
            continue

        try:
            with lzma.open(fstaging, mode="rt", encoding="utf-8") as source:
                decompressed_data = source.read()
        except (lzma.LZMAError, UnicodeDecodeError) as err:
            print(f"Failed to decompress {fstaging}: {err}")
            continue

        with fraw.open("w") as target:
            target.write(decompressed_data)
        print(f"{fraw} decompressed")


def delete_files():
    base_folder = data_path("staging")
    shutil.rmtree(base_folder, ignore_errors=True)


decompress_files(folder, file, start_date, end_date)

if delete == "y":
    delete_files()
