# ObsCop 2025 avec Redis

Ce projet importe `ObsCop2025.csv` dans RedisJSON et permet de lire les lignes ou de mesurer les performances de Redis.

## Prérequis

- Python 3
- Docker avec le daemon en fonctionnement
- Le fichier `ObsCop2025.csv` dans le dossier du projet

Le script utilise RedisJSON; l’image Redis Stack le fournit.

## Installation

Depuis le dossier du projet dans WSL, crée et active l’environnement Python, puis installe le client Redis :

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Démarre Redis Stack la première fois :

```bash
docker run -d --name redis-stack -p 6379:6379 redis/redis-stack-server:latest
```

Pour redémarrer le conteneur ensuite :

```bash
docker start redis-stack
```

Vérifie qu’il répond :

```bash
docker exec redis-stack redis-cli ping
```

La réponse attendue est `PONG`.

## Importer les données

Depuis la racine du projet :

```bash
python main.py import
```

L’import lit le CSV en UTF-8, séparé par des points-virgules, et écrit les lignes par lots de 500. Chaque ligne est enregistrée en JSON sous une clé `obscop2025:row:<numéro>`. Sans sous-commande, `python main.py` lance également l’import.

## Lire les données

Afficher la ligne 1 :

```bash
python main.py get 1
```

Afficher les 10 premières lignes :

```bash
python main.py list --start 1 --limit 10
```

`--start` et `--limit` doivent être des entiers positifs.

## Lancer le benchmark

```bash
python main.py benchmark --rows 1000 --batch-size 500
```

Pour un essai rapide, utilise par exemple `--rows 10`. Le benchmark prend des lignes du CSV, mesure les écritures RedisJSON par pipeline, puis compare les lectures `GET` séquentielles et en pipeline. Il utilise des clés temporaires et les supprime à la fin. Il n’est pas nécessaire d’importer tout le CSV avant de lancer le benchmark.

Les mesures dépendent de la machine, de la charge et de la connexion à Redis.

## Comparer Redis et PostgreSQL

Le benchmark ci-dessus compare Redis à lui-même (séquentiel contre pipeline). Pour mesurer ce que Redis apporte face à une base classique, `compare` charge les mêmes lignes dans PostgreSQL (table `obscop2025`, colonne `jsonb`, clé primaire `id`) et interroge les deux bases avec le même client Python.

Démarre un PostgreSQL dédié la première fois :

```bash
docker run -d --name obscop-pg -p 5434:5432 \
  -e POSTGRES_USER=obscop -e POSTGRES_PASSWORD=obscop -e POSTGRES_DB=obscop \
  postgres:16 -c shared_buffers=256MB -c synchronous_commit=off \
  -c shared_preload_libraries=pg_stat_statements -c max_connections=200
docker exec obscop-pg psql -U obscop -d obscop -c 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements'
```

`synchronous_commit=off` aligne la durabilité sur celle de Redis par défaut : un commit n’attend pas l’écriture sur disque.

Puis lance la comparaison :

```bash
python main.py compare --rows 10000 --clients 1 8 32 --duration 3 --runs 3 --output resultats.json
```

Elle réimporte tout le CSV dans les deux bases (pipeline `JSON.SET` pour Redis, `COPY` pour PostgreSQL) puis mesure :

1. l’import complet ;
2. la lecture par clé avec un client : latences p50/p95/p99, débit, lecture par lots (`pipeline` contre `WHERE id = ANY(...)`) et temps passé côté serveur (`INFO commandstats`, `pg_stat_statements`) ;
3. la lecture par clé avec plusieurs clients en parallèle ;
4. Redis en cache devant une requête PostgreSQL coûteuse (préoccupations par pays, question A1BIS).

Par défaut, le script se connecte à `postgresql://obscop:obscop@localhost:5434/obscop` ; définis `POSTGRES_URL` pour en changer.

## Configuration Redis

Par défaut, le script se connecte à `redis://localhost:6379`. Pour utiliser une autre adresse, définis `REDIS_URL` dans le terminal avant de lancer le script :

```bash
export REDIS_URL='redis://localhost:6379'
```
