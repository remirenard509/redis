import csv
import json
import os
from pathlib import Path

import redis


DATA_FILE = Path(__file__).resolve().parent / "ObsCop2025.csv"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
BATCH_SIZE = 500


def redis_key(row_number):
    return f"obscop2025:row:{row_number}"


def main():
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    client.ping()

    imported_count = 0
    pipeline = client.pipeline(transaction=False)

    with DATA_FILE.open(encoding="utf-8-sig", newline="") as data_file:
        records = csv.DictReader(data_file, delimiter=";")
        if records.fieldnames is None:
            raise ValueError(f"Le fichier {DATA_FILE.name} ne contient pas d'en-tête CSV.")

        for row_number, record in enumerate(records, start=1):
            pipeline.execute_command(
                "JSON.SET",
                redis_key(row_number),
                "$",
                json.dumps(record, ensure_ascii=False),
            )
            imported_count += 1

            if imported_count % BATCH_SIZE == 0:
                pipeline.execute()

    if imported_count % BATCH_SIZE:
        pipeline.execute()

    print(f"{imported_count} lignes importées depuis {DATA_FILE.name} dans Redis.")


if __name__ == "__main__":
    main()