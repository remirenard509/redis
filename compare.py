"""Comparer Redis et PostgreSQL sur les données ObsCop 2025.

Les deux bases reçoivent les mêmes lignes et sont interrogées par le même client
Python, sur le même chemin réseau (conteneurs Docker en local). Seule la base change.
"""
import csv
import json
import multiprocessing
import os
import platform
import random
import statistics
import time

import psycopg
import redis

from main import DATA_FILE, REDIS_URL, redis_key


POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://obscop:obscop@localhost:5434/obscop")
TABLE = "obscop2025"
POINT_QUERY = f"SELECT data::text FROM {TABLE} WHERE id = %s"
BATCH_QUERY = f"SELECT data::text FROM {TABLE} WHERE id = ANY(%s)"
# Requête de tableau de bord : les préoccupations par pays (question A1BIS).
# Aucun index ne la couvre : PostgreSQL parcourt toute la table à chaque appel.
DASHBOARD_QUERY = f"""
SELECT data->>'Population_en', data->>'Answer', replace(data->>'Valeur', ',', '.')::float
FROM {TABLE}
WHERE data->>'Question_en' LIKE 'A1BIS.%'
  AND trim(data->>'Filter') = 'Country'
  AND data->>'Base_en' = 'ALL'
ORDER BY 1, 3 DESC
"""
DASHBOARD_CACHE_KEY = "cache:obscop2025:a1bis-par-pays"
CACHE_TTL_SECONDS = 60
MEBIBYTE = 1024 * 1024


def read_records():
    with DATA_FILE.open(encoding="utf-8-sig", newline="") as data_file:
        return list(csv.DictReader(data_file, delimiter=";"))


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]


def latency_summary(latencies, seconds):
    return {
        "ops_per_s": len(latencies) / seconds,
        "p50_us": percentile(latencies, 0.50) * 1e6,
        "p95_us": percentile(latencies, 0.95) * 1e6,
        "p99_us": percentile(latencies, 0.99) * 1e6,
    }


def median_of(runs):
    return {key: statistics.median(run[key] for run in runs) for key in runs[0]}


def wait_for_redis_snapshot(client):
    # Avec la config « save » par défaut, un import déclenche un snapshot RDB en
    # tâche de fond : on attend sa fin pour ne pas mesurer pendant le fork.
    time.sleep(1)
    while client.info("persistence")["rdb_bgsave_in_progress"]:
        time.sleep(0.2)


def import_redis(client, records, batch_size):
    keys = [redis_key(row_number) for row_number in range(1, len(records) + 1)]
    for offset in range(0, len(keys), batch_size):
        client.unlink(*keys[offset:offset + batch_size])

    started_at = time.perf_counter()
    for offset in range(0, len(records), batch_size):
        pipeline = client.pipeline(transaction=False)
        for row_number, record in enumerate(records[offset:offset + batch_size], start=offset + 1):
            pipeline.execute_command(
                "JSON.SET",
                redis_key(row_number),
                "$",
                json.dumps(record, ensure_ascii=False),
            )
        pipeline.execute()
    seconds = time.perf_counter() - started_at

    wait_for_redis_snapshot(client)
    return seconds


def import_postgres(connection, records):
    connection.execute(f"CREATE TABLE IF NOT EXISTS {TABLE} (id integer PRIMARY KEY, data jsonb NOT NULL)")
    connection.execute(f"TRUNCATE {TABLE}")

    started_at = time.perf_counter()
    with connection.cursor() as cursor, cursor.copy(f"COPY {TABLE} (id, data) FROM STDIN") as copy:
        for row_number, record in enumerate(records, start=1):
            copy.write_row((row_number, json.dumps(record, ensure_ascii=False)))
    seconds = time.perf_counter() - started_at

    # Comme pour le snapshot Redis : rien ne doit tourner en fond pendant les lectures.
    connection.execute(f"VACUUM ANALYZE {TABLE}")
    connection.execute("CHECKPOINT")
    return seconds


def redis_reader(url):
    client = redis.Redis.from_url(url, decode_responses=True)
    return lambda row_id: client.execute_command("JSON.GET", redis_key(row_id))


def postgres_reader(url):
    cursor = psycopg.connect(url, autocommit=True).cursor()

    def read(row_id):
        cursor.execute(POINT_QUERY, (row_id,))
        return cursor.fetchone()

    return read


READERS = {"redis": (redis_reader, REDIS_URL), "postgres": (postgres_reader, POSTGRES_URL)}


def sequential_reads(read, ids):
    latencies = []
    started_at = time.perf_counter()
    for row_id in ids:
        read_started_at = time.perf_counter()
        read(row_id)
        latencies.append(time.perf_counter() - read_started_at)
    return latency_summary(latencies, time.perf_counter() - started_at)


def redis_batch_reads(client, ids, batch_size):
    started_at = time.perf_counter()
    for offset in range(0, len(ids), batch_size):
        pipeline = client.pipeline(transaction=False)
        for row_id in ids[offset:offset + batch_size]:
            pipeline.execute_command("JSON.GET", redis_key(row_id))
        pipeline.execute()
    return len(ids) / (time.perf_counter() - started_at)


def postgres_batch_reads(connection, ids, batch_size):
    started_at = time.perf_counter()
    for offset in range(0, len(ids), batch_size):
        connection.execute(BATCH_QUERY, (ids[offset:offset + batch_size],)).fetchall()
    return len(ids) / (time.perf_counter() - started_at)


def postgres_server_ms(connection, query_pattern):
    row = connection.execute(
        "SELECT sum(total_exec_time) / sum(calls) FROM pg_stat_statements WHERE query LIKE %s",
        (query_pattern,),
    ).fetchone()
    return row[0]


def point_reads(client, connection, ids, batch_size):
    redis_read = redis_reader(REDIS_URL)
    postgres_read = postgres_reader(POSTGRES_URL)
    for row_id in ids:  # échauffement : données en mémoire, requête préparée
        redis_read(row_id)
        postgres_read(row_id)

    client.config_resetstat()
    connection.execute("SELECT pg_stat_statements_reset()")
    redis_result = sequential_reads(redis_read, ids)
    postgres_result = sequential_reads(postgres_read, ids)
    redis_result["server_us"] = client.info("commandstats")["cmdstat_json.get"]["usec_per_call"]
    postgres_result["server_us"] = postgres_server_ms(connection, f"{POINT_QUERY.replace('%s', '$1')}%") * 1000

    redis_result["batch_ops_per_s"] = redis_batch_reads(client, ids, batch_size)
    postgres_result["batch_ops_per_s"] = postgres_batch_reads(connection, ids, batch_size)
    return redis_result, postgres_result


def concurrent_reader(backend, ids, duration, barrier, results):
    make_reader, url = READERS[backend]
    read = make_reader(url)
    for row_id in ids[:100]:  # échauffement : connexion ouverte, requête préparée
        read(row_id)
    barrier.wait()

    latencies = []
    deadline = time.perf_counter() + duration
    index = 0
    while True:
        started_at = time.perf_counter()
        if started_at >= deadline:
            break
        read(ids[index % len(ids)])
        latencies.append(time.perf_counter() - started_at)
        index += 1
    results.put(latencies)


def concurrent_reads(backend, client_count, ids, duration):
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(client_count)
    results = context.Queue()
    workers = [
        context.Process(
            target=concurrent_reader,
            args=(backend, random.Random(index).sample(ids, len(ids)), duration, barrier, results),
        )
        for index in range(client_count)
    ]
    for worker in workers:
        worker.start()
    latencies = [latency for _ in workers for latency in results.get()]
    for worker in workers:
        worker.join()
    return latency_summary(latencies, duration)


def dashboard_cache(client, connection, query_count, hit_count):
    def query_postgres():
        return connection.execute(DASHBOARD_QUERY).fetchall()

    def cached_dashboard():
        value = client.get(DASHBOARD_CACHE_KEY)
        if value is not None:
            return json.loads(value)
        rows = query_postgres()
        client.set(DASHBOARD_CACHE_KEY, json.dumps(rows, ensure_ascii=False), ex=CACHE_TTL_SECONDS)
        return rows

    def median_ms(action, count, before=None):
        durations = []
        for _ in range(count):
            if before:
                before()
            started_at = time.perf_counter()
            action()
            durations.append(time.perf_counter() - started_at)
        return statistics.median(durations) * 1000

    rows = query_postgres()
    connection.execute("SELECT pg_stat_statements_reset()")
    postgres_ms = median_ms(query_postgres, query_count)
    server_ms = postgres_server_ms(connection, "%replace(data->>%")  # constantes normalisées en $n
    miss_ms = median_ms(cached_dashboard, query_count, before=lambda: client.delete(DASHBOARD_CACHE_KEY))
    cached_dashboard()
    hit_ms = median_ms(cached_dashboard, hit_count)
    client.delete(DASHBOARD_CACHE_KEY)

    return {
        "rows": len(rows),
        "cached_bytes": len(json.dumps(rows, ensure_ascii=False).encode()),
        "postgres_ms": postgres_ms,
        "postgres_server_ms": server_ms,
        "miss_ms": miss_ms,
        "hit_ms": hit_ms,
        "mean_ms_at_90": 0.9 * hit_ms + 0.1 * miss_ms,
        "mean_ms_at_99": 0.99 * hit_ms + 0.01 * miss_ms,
    }


def compare(client, row_count, batch_size, client_counts, duration, runs, output=None):
    records = read_records()
    connection = psycopg.connect(POSTGRES_URL, autocommit=True)
    ids = random.Random(42).sample(range(1, len(records) + 1), min(row_count, len(records)))
    results = {
        "conditions": {
            "machine": platform.machine(),
            "python": platform.python_version(),
            "redis_py": redis.__version__,
            "psycopg": psycopg.__version__,
            "redis_server": client.info("server")["redis_version"],
            "postgres_server": connection.execute("SHOW server_version").fetchone()[0],
            "rows": len(records),
            "sample": len(ids),
            "batch_size": batch_size,
            "runs": runs,
            "duration_s": duration,
        },
    }

    print(f"1. Import complet de {len(records)} lignes (médiane de {runs} passes)")
    import_runs = [
        {"redis_s": import_redis(client, records, batch_size), "postgres_s": import_postgres(connection, records)}
        for _ in range(runs)
    ]
    imported = median_of(import_runs)
    imported["redis_memory_bytes"] = client.info("memory")["used_memory"]
    imported["postgres_size_bytes"] = connection.execute(
        f"SELECT pg_total_relation_size('{TABLE}')"
    ).fetchone()[0]
    results["import"] = imported
    print(
        f"   Redis      pipeline JSON.SET : {imported['redis_s']:.2f} s, "
        f"{len(records) / imported['redis_s']:,.0f} lignes/s, {imported['redis_memory_bytes'] / MEBIBYTE:.0f} Mo en RAM"
    )
    print(
        f"   PostgreSQL COPY jsonb        : {imported['postgres_s']:.2f} s, "
        f"{len(records) / imported['postgres_s']:,.0f} lignes/s, {imported['postgres_size_bytes'] / MEBIBYTE:.0f} Mo sur disque"
    )

    print(f"\n2. Lecture par clé, 1 client : {len(ids)} clés aléatoires (médiane de {runs} passes)")
    point_runs = [point_reads(client, connection, ids, batch_size) for _ in range(runs)]
    results["point_reads"] = {
        "redis": median_of([redis_run for redis_run, _ in point_runs]),
        "postgres": median_of([postgres_run for _, postgres_run in point_runs]),
    }
    print(f"   {'':<11}{'p50':>9}{'p95':>9}{'p99':>9}{'débit':>12}{f'lots de {batch_size}':>14}{'serveur':>10}")
    for label, backend in (("Redis", "redis"), ("PostgreSQL", "postgres")):
        result = results["point_reads"][backend]
        print(
            f"   {label:<11}{result['p50_us']:>6.0f} µs{result['p95_us']:>6.0f} µs{result['p99_us']:>6.0f} µs"
            f"{result['ops_per_s']:>10,.0f}/s{result['batch_ops_per_s']:>12,.0f}/s{result['server_us']:>7.1f} µs"
        )

    print(f"\n3. Lectures par clé concurrentes : {duration} s par mesure (médiane de {runs} passes)")
    results["concurrency"] = {}
    print(f"   {'clients':>7}{'Redis':>14}{'p99':>9}{'PostgreSQL':>14}{'p99':>9}")
    for client_count in client_counts:
        measured = {
            backend: median_of([concurrent_reads(backend, client_count, ids, duration) for _ in range(runs)])
            for backend in ("redis", "postgres")
        }
        results["concurrency"][str(client_count)] = measured
        print(
            f"   {client_count:>7}{measured['redis']['ops_per_s']:>12,.0f}/s{measured['redis']['p99_us']:>6.0f} µs"
            f"{measured['postgres']['ops_per_s']:>12,.0f}/s{measured['postgres']['p99_us']:>6.0f} µs"
        )

    cache = dashboard_cache(client, connection, query_count=30, hit_count=3000)
    results["cache"] = cache
    print(f"\n4. Redis en cache devant PostgreSQL : préoccupations par pays (A1BIS), {cache['rows']} lignes")
    print(f"   PostgreSQL à chaque appel  : {cache['postgres_ms']:.2f} ms (dont {cache['postgres_server_ms']:.2f} ms côté serveur)")
    print(f"   Cache manqué (requête + SET) : {cache['miss_ms']:.2f} ms")
    print(f"   Cache trouvé (GET + json)    : {cache['hit_ms']:.3f} ms")
    print(f"   Moyenne à 90 % / 99 % de hits : {cache['mean_ms_at_90']:.2f} ms / {cache['mean_ms_at_99']:.2f} ms")

    if output:
        output.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nRésultats détaillés écrits dans {output}.")
