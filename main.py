import json
import os
from pathlib import Path
from urllib.parse import quote

import redis


DATA_FILE = Path(__file__).resolve().parent / "resultats-elections-legislatives-tour2.json"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


def redis_key(record):
    candidate = quote(str(record["CANDIDAT"]), safe="")
    return (
        f"election:{record['CODE_ELECTION']}:tour:{record['NUMRO_TOUR']}"
        f":lieu:{record['NUMERO_LIEU']}:candidat:{candidate}"
    )


def main():
    with DATA_FILE.open(encoding="utf-8") as data_file:
        records = json.load(data_file)

    if not isinstance(records, list):
        raise ValueError(f"Le fichier {DATA_FILE.name} doit contenir une liste de résultats.")

    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    client.ping()

    for record in records:
        client.json().set(redis_key(record), "$", record)

    print(f"{len(records)} résultats importés dans Redis.")


if __name__ == "__main__":
    main()