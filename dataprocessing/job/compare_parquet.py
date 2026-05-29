import hashlib
import os
import sys
from argparse import ArgumentParser
from pathlib import Path

PACKAGE_PARENT = ".."
SCRIPT_DIR = os.path.dirname(
    os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
)
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from dataprocessing.processors.sparketl import ETLSpark


def directory_checksum(root_path: str) -> str:
    root = Path(root_path)
    if not root.exists():
        raise FileNotFoundError(root_path)

    digest = hashlib.sha256()
    file_paths = sorted(p for p in root.rglob("*") if p.is_file())

    for file_path in file_paths:
        relative_path = file_path.relative_to(root).as_posix()
        file_digest = hashlib.sha256()

        with file_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                file_digest.update(chunk)

        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(file_digest.hexdigest().encode("ascii"))
        digest.update(b"\n")

    return digest.hexdigest()


def flat_file_checksum(root_path: str) -> str:
    root = Path(root_path)
    if not root.exists():
        raise FileNotFoundError(root_path)

    digest = hashlib.sha256()
    file_hashes = []

    for file_path in sorted(p for p in root.rglob("*") if p.is_file()):
        file_digest = hashlib.sha256()
        with file_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                file_digest.update(chunk)
        file_hashes.append(file_digest.hexdigest())

    for file_hash in sorted(file_hashes):
        digest.update(file_hash.encode("ascii"))
        digest.update(b"\n")

    return digest.hexdigest()


def main():
    parser = ArgumentParser(
        description="Compare two parquet datasets for schema, row, or byte equality"
    )
    parser.add_argument("left", help="Left parquet path")
    parser.add_argument("right", help="Right parquet path")
    parser.add_argument(
        "--mode",
        choices=("rows", "bytes", "flat-bytes"),
        default="rows",
        help="rows compares Spark content; bytes compares exact tree layout and bytes; flat-bytes ignores layout and compares the multiset of file bytes",
    )
    args = parser.parse_args()

    if args.mode == "bytes":
        left_checksum = directory_checksum(args.left)
        right_checksum = directory_checksum(args.right)

        print(f"LEFT:  {args.left}")
        print(f"RIGHT: {args.right}")
        print(f"LEFT CHECKSUM:  {left_checksum}")
        print(f"RIGHT CHECKSUM: {right_checksum}")

        if left_checksum == right_checksum:
            print("RESULT: MATCH")
            return 0

        print("RESULT: DIFFERENT")
        return 1

    if args.mode == "flat-bytes":
        left_checksum = flat_file_checksum(args.left)
        right_checksum = flat_file_checksum(args.right)

        print(f"LEFT:  {args.left}")
        print(f"RIGHT: {args.right}")
        print(f"LEFT CHECKSUM:  {left_checksum}")
        print(f"RIGHT CHECKSUM: {right_checksum}")

        if left_checksum == right_checksum:
            print("RESULT: MATCH")
            return 0

        print("RESULT: DIFFERENT")
        return 1

    spark = ETLSpark().sqlContext
    left_df = spark.read.parquet(args.left)
    right_df = spark.read.parquet(args.right)

    print(f"LEFT:  {args.left}")
    print(f"RIGHT: {args.right}")
    print(f"LEFT ROWS:  {left_df.count()}")
    print(f"RIGHT ROWS: {right_df.count()}")

    if left_df.schema != right_df.schema:
        print("SCHEMA: DIFFERENT")
        print("LEFT SCHEMA:")
        print(left_df.schema.simpleString())
        print("RIGHT SCHEMA:")
        print(right_df.schema.simpleString())
        return 1

    left_only = left_df.exceptAll(right_df)
    right_only = right_df.exceptAll(left_df)

    left_only_count = left_only.count()
    right_only_count = right_only.count()

    if left_only_count == 0 and right_only_count == 0:
        print("RESULT: MATCH")
        return 0

    print("RESULT: DIFFERENT")
    print(f"ROWS ONLY IN LEFT:  {left_only_count}")
    print(f"ROWS ONLY IN RIGHT: {right_only_count}")

    if left_only_count:
        print("SAMPLE ROWS ONLY IN LEFT:")
        left_only.show(20, truncate=False)

    if right_only_count:
        print("SAMPLE ROWS ONLY IN RIGHT:")
        right_only.show(20, truncate=False)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
