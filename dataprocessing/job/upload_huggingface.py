import os
import sys
from argparse import ArgumentParser

from huggingface_hub import HfApi

# in a run, only a month will be processed at a time. at the end of refined_processing.py we upload the files related to that month
# the yyyy-mm is defined in the script from the job, it is passed as an argument to this script as a date string
#
# given a yyyy-mm i want to upload the files related to that month
# data/refined/bus_tracking/year=yyyy/month=mm (note that it can be a single digit month, i.e. month=1)
# data/refined/bus_itineraries/year=yyyy/month=mm (note that it can be a single digit month, i.e. month=1)
# data/refined/bus_lines/year=yyyy/month=mm (note that it can be a single digit month, i.e. month=1)
#
# the hugging face repo to upload to is curitibaresearch/raw-bus-transit
# the repo path is bus_tracking, bus_itineraries, or bus_lines + year=yyyy/month=mm
# the type of the repo is a dataset
#
# authentication is done using the HF_TOKEN environment variable


sys.path.insert(0, os.path.abspath(".."))


def load_env_file(path: str) -> None:
    if not os.path.isfile(path):
        return
    with open(path, "r", encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].lstrip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


parser = ArgumentParser()
parser.add_argument(
    "-d",
    "--date",
    dest="date",
    help="date (yyyy-mm)",
    metavar="DATE",
    required=True,
)

args = parser.parse_args()

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_env_file(os.path.join(repo_root, ".env"))
load_env_file(os.path.join(os.getcwd(), ".env"))

token = os.environ.get("HF_TOKEN")
if not token:
    print(
        "HF_TOKEN environment variable is required for authentication.", file=sys.stderr
    )
    sys.exit(1)

try:
    year_str, month_str = args.date.split("-")
    year = int(year_str)
    month_int = int(month_str)
except ValueError:
    print("Invalid date format. Expected yyyy-mm.", file=sys.stderr)
    sys.exit(1)

if month_int < 1 or month_int > 12 or len(year_str) != 4:
    print(
        "Invalid date. Expected yyyy-mm with month between 01 and 12.", file=sys.stderr
    )
    sys.exit(1)

month_candidates = [str(month_int)]
if month_str != str(month_int):
    month_candidates.append(month_str)

api = HfApi(token=token)
repo_id = "curitibaresearch/bus-interpolated"
datasets = ["bus_tracking", "bus_itineraries", "bus_lines"]

for dataset in datasets:
    local_path = None
    month_dir = None
    for candidate in month_candidates:
        candidate_path = os.path.join(
            "data",
            "refined",
            dataset,
            f"year={year_str}",
            f"month={candidate}",
        )
        if os.path.isdir(candidate_path):
            local_path = candidate_path
            month_dir = candidate
            break

    if not local_path or not month_dir:
        print(
            f"Missing local data for {dataset} {year_str}-{month_str}.",
            file=sys.stderr,
        )
        sys.exit(1)

    api.upload_folder(
        folder_path=local_path,
        path_in_repo=f"{dataset}/year={year_str}/month={month_dir}",
        repo_id=repo_id,
        repo_type="dataset",
        commit_message=f"Upload {dataset} {year_str}-{month_dir}",
        ignore_patterns=["*.parquet.crc", "**/*.parquet.crc"],
    )
