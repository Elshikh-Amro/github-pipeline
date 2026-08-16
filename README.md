# GitHub Analytics Pipeline

End-to-end data engineering pipeline that ingests GitHub data (top Python repos, commits, issues), transforms it into a dimensional star schema, orchestrates runs with Prefect, and visualizes metrics in Metabase.

## Architecture

```
GitHub API → Python (ingest) → PostgreSQL → dbt (star schema) → Metabase
                                    ↑
                             Prefect (orchestrates)
```

## Tech Stack

| Layer | Tool |
|-------|------|
| Ingestion | Python (`requests`, `psycopg`) |
| Warehouse | PostgreSQL 16 |
| Transform | dbt-core + dbt-postgres |
| Orchestration | Prefect 3.x |
| BI / Dashboards | Metabase |
| Infra | Docker Compose |

## Project Structure

```
github-pipline/
├── docker-compose.yml       — postgres, prefect-server, worker, metabase
├── Dockerfile               — worker image (Python + dbt + prefect)
├── Makefile                 — one-command targets (up, all, setup-metabase)
├── .env                     — GITHUB_TOKEN (gitignored)
├── ingest/                  — Week 1 EL: github_api.py, db.py, main.py
├── flows/                   — Week 3 Prefect flow + deployment
├── dbt_transforms/          — Week 2 dbt project (staging + marts)
├── metabase/provision.py    — Week 4 auto-provision dashboards
└── docker/initdb/           — creates `metabase` DB on fresh volumes
```

## Quick Start

```bash
# 1. Prerequisites: Docker Desktop running, git, Python 3.12
git clone https://github.com/Elshikh-Amro/github-pipeline.git
cd github-pipline

# 2. Create .env (gitignored) with your GitHub token
echo 'GITHUB_TOKEN=ghp_xxx' > .env

# 3. Build the worker image (installs dbt + prefect + deps)
docker compose build worker

# 4. Start everything (postgres → ensure metabase DB → prefect server → worker → metabase)
make up

# 5. Run the pipeline once (fetch → load → dbt build → source freshness)
docker compose exec worker python -c "from flows.ingestion_flow import ingestion_pipeline; ingestion_pipeline()"

# 6. Provision Metabase (first time only; idempotent afterwards)
make setup-metabase
```

### `make all`

Starts all services (same as `make up`), waits, and reminds you where things live. The pipeline runs automatically on the daily 6am cron, or trigger it manually:

```bash
docker compose exec worker python -c "from flows.ingestion_flow import ingestion_pipeline; ingestion_pipeline()"
```

## UIs & Credentials

| Service | URL | Credentials |
|---------|-----|-------------|
| Prefect | http://localhost:4200 | — |
| Metabase | http://localhost:3000 | `admin@example.com` / `metabase-pass-123` (override via `METABASE_EMAIL`/`METABASE_PASSWORD`) |
| PostgreSQL | localhost:5432 | `postgres` / `postgres`, db `github_analytics` |

## Make Targets

| Target | What it does |
|--------|--------------|
| `make up` | Start postgres first, ensure `metabase` DB exists, then start all services |
| `make all` | `make up` + friendly status output |
| `make run` | Trigger the Prefect deployment once |
| `make deploy` | (Re)serve the deployment in the worker |
| `make setup-metabase` | Auto-provision Metabase DB connection, cards, dashboard |
| `make logs` | Follow worker logs |

## Testing Every Part

### 1. Infrastructure

```bash
docker compose ps                     # all 4 services Up; postgres/prefect (healthy)
curl -s localhost:3000/api/health     # {"status":"ok"}
curl -s localhost:4200/api/health     # true
docker compose exec postgres psql -U postgres -c \
  "SELECT datname FROM pg_database WHERE datname='metabase';"
```

### 2. Ingestion (Week 1)

```bash
# Standalone (no Prefect), in a local conda env:
conda activate de-pipeline && python -m ingest.main

# Or inside the container:
docker compose exec worker python -m ingest.main
```

Verify raw tables:
```bash
docker compose exec postgres psql -U postgres -d github_analytics -c \
  "SELECT (SELECT COUNT(*) FROM raw_repos) repos,
          (SELECT COUNT(*) FROM raw_commits) commits,
          (SELECT COUNT(*) FROM raw_issues) issues;"
```

Test the API client directly:
```bash
docker compose exec worker python -c "
from ingest.github_api import fetch_top_repos
r = fetch_top_repos(3)
print(len(r), 'repos; top:', r[0]['name'], r[0]['stars'])"
```

### 3. dbt Transforms (Week 2)

```bash
docker compose exec worker dbt debug                 # connection OK?
docker compose exec worker dbt run                   # staging views + marts
docker compose exec worker dbt test                  # 24 schema tests
docker compose exec worker dbt source freshness      # freshness config
docker compose exec worker dbt docs generate && docker compose exec worker dbt docs serve
```

Verify marts:
```bash
docker compose exec postgres psql -U postgres -d github_analytics -c "\dt public_marts.*"
docker compose exec postgres psql -U postgres -d github_analytics -c \
  "SELECT repo_name, stars FROM public_marts.dim_repositories ORDER BY stars DESC LIMIT 5;"
docker compose exec postgres psql -U postgres -d github_analytics -c \
  "SELECT repo_name, COUNT(*) FILTER (WHERE is_closed) closed,
          COUNT(*) FILTER (WHERE NOT is_closed) open
   FROM public_marts.fact_issues GROUP BY 1 ORDER BY closed DESC LIMIT 5;"
```

### 4. Prefect Orchestration (Week 3)

```bash
make run                              # trigger deployment 'ingestion-pipeline/github-pipeline'
make logs                             # watch the flow run in the worker
docker compose exec worker prefect deployment ls
```

Verify in the Prefect UI (http://localhost:4200): Flow Runs → completed with success; check the 6am cron schedule and that `dbt build` + `dbt source freshness` tasks completed.

### 5. Metabase (Week 4)

```bash
make setup-metabase                   # idempotent; creates DB, 3 cards, dashboard
```

Verify: open http://localhost:3000/dashboard/2 — **Trending Repos**, **Commit Velocity**, and **Issue Closure by Repo** should render with data. Confirm the source DB is connected under Admin → Databases → "GitHub Analytics".

### 6. Failure Paths / Retries

```bash
docker compose restart worker         # deployment re-registers (healthcheck prevents startup race)
docker compose exec worker python -c \
  "from flows.ingestion_flow import ingestion_pipeline; ingestion_pipeline(num_repos=3, run_dbt=True)"  # small smoke test
```

Each `ingestion_pipeline()` run upserts by primary key, so repeated runs are safe and grow the dataset.

## Data Model

Star schema in `public_marts`:

| Table | Type | Description |
|-------|------|-------------|
| `dim_repositories` | Dimension | Unique repos (id, name, owner, stars, language) |
| `dim_dates` | Dimension | Calendar dates from commits/issues (`date_key`) |
| `fact_commits` | Fact | Commit facts linked to repo + date dims |
| `fact_issues` | Fact | Issue facts with `is_closed`, `days_to_close` |

## Roadmap

- [x] Week 1 — EL pipeline (fetch GitHub data → PostgreSQL raw tables)
- [x] Week 2 — dbt staging + marts, tests, docs
- [x] Week 3 — Prefect flows, retries, cron scheduling
- [x] Week 4 — Metabase dashboards, source freshness, Docker Compose for everything
- [ ] Stretch — Great Expectations, GitHub Actions CI/CD, Redpanda streaming
