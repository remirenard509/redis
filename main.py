import argparse
import csv
import json
import os
from pathlib import Path
import time
import uuid

import redis


DATA_FILE = Path(__file__).resolve().parent / "ObsCop2025.csv"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
BATCH_SIZE = 500


def redis_key(row_number):
    return f"obscop2025:row:{row_number}"


def import_data(client):
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


def benchmark(client, row_count, batch_size):
    with DATA_FILE.open(encoding="utf-8-sig", newline="") as data_file:
        reader = csv.DictReader(data_file, delimiter=";")
        records = []
        for record in reader:
            records.append(record)
            if len(records) == row_count:
                break

    if not records:
        raise ValueError(f"Aucune donnée trouvée dans {DATA_FILE.name}.")

    namespace = f"benchmark:obscop2025:{uuid.uuid4().hex}"
    keys = [f"{namespace}:{index}" for index in range(len(records))]
    try:
        started_at = time.perf_counter()
        for offset in range(0, len(records), batch_size):
            pipeline = client.pipeline(transaction=False)
            for key, record in zip(keys[offset:offset + batch_size], records[offset:offset + batch_size]):
                pipeline.execute_command(
                    "JSON.SET",
                    key,
                    "$",
                    json.dumps(record, ensure_ascii=False),
                )
            pipeline.execute()
        write_seconds = time.perf_counter() - started_at

        started_at = time.perf_counter()
        for key in keys:
            client.execute_command("JSON.GET", key)
        sequential_read_seconds = time.perf_counter() - started_at

        started_at = time.perf_counter()
        for offset in range(0, len(keys), batch_size):
            pipeline = client.pipeline(transaction=False)
            for key in keys[offset:offset + batch_size]:
                pipeline.execute_command("JSON.GET", key)
            pipeline.execute()
        pipelined_read_seconds = time.perf_counter() - started_at
    finally:
        for offset in range(0, len(keys), batch_size):
            client.unlink(*keys[offset:offset + batch_size])

    print(f"Benchmark RedisJSON sur {len(records)} lignes (lot de {batch_size})")
    for label, duration in (
        ("Écriture pipeline", write_seconds),
        ("Lecture GET séquentielle", sequential_read_seconds),
        ("Lecture GET pipeline", pipelined_read_seconds),
    ):
        rate = len(records) / duration if duration else float("inf")
        print(f"{label}: {duration:.3f} s, {rate:,.0f} opérations/s")


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("la valeur doit être supérieure à zéro")
    return number


def create_parser():
    parser = argparse.ArgumentParser(description="Importer ou lire les données ObsCop 2025 dans Redis.")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("import", help="importe le CSV dans Redis")

    get_parser = commands.add_parser("get", help="affiche une ligne par son numéro")
    get_parser.add_argument("row_number", type=positive_int)

    list_parser = commands.add_parser("list", help="affiche une plage de lignes")
    list_parser.add_argument("--start", type=positive_int, default=1)
    list_parser.add_argument("--limit", type=positive_int, default=10)

    benchmark_parser = commands.add_parser("benchmark", help="mesure les performances de RedisJSON")
    benchmark_parser.add_argument("--rows", type=positive_int, default=1000)
    benchmark_parser.add_argument("--batch-size", type=positive_int, default=BATCH_SIZE)
    return parser


def main():
    args = create_parser().parse_args()
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    client.ping()

    if args.command in (None, "import"):
        import_data(client)
    elif args.command == "get":
        record = client.json().get(redis_key(args.row_number))
        if record is None:
            raise SystemExit(f"Aucune donnée trouvée pour la ligne {args.row_number}.")
        print(json.dumps(record, ensure_ascii=False, indent=2))
    elif args.command == "list":
        records = []
        for row_number in range(args.start, args.start + args.limit):
            record = client.json().get(redis_key(row_number))
            if record is not None:
                records.append({"ligne": row_number, "donnees": record})
        print(json.dumps(records, ensure_ascii=False, indent=2))
    elif args.command == "benchmark":
        benchmark(client, args.rows, args.batch_size)


if __name__ == "__main__":
    main()