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

## Configuration Redis

Par défaut, le script se connecte à `redis://localhost:6379`. Pour utiliser une autre adresse, définis `REDIS_URL` dans le terminal avant de lancer le script :

```bash
export REDIS_URL='redis://localhost:6379'
```
