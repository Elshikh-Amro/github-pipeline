# Week 1 — EL Pipeline

## Tools Learned
- **Docker Compose** — ran PostgreSQL 16 locally in a container
- **Python** — `requests` for API calls, `psycopg` for database connection
- **GitHub REST API** — fetched repos (search), commits, issues
- **SQL** — CREATE TABLE, INSERT, SELECT, ON CONFLICT (upsert)

## Schema (Raw Layer)
| Table | Rows | Content |
|-------|------|---------|
| `raw_repos` | 50 | id, name, owner, stars, forks, language, description, url |
| `raw_commits` | ~500-1000 | sha, repo_name, author, message, committed_at |
| `raw_issues` | ~200-500 | id, repo_name, title, state, created_at |

## Project Structure
github-pipline/
├── docker-compose.yml    — PostgreSQL service
├── requirements.txt      — Python dependencies
├── .gitignore
├── .env                  — GITHUB_TOKEN
├── ingest/
│   ├── init.py
│   ├── github_api.py     — API client (fetch repos, commits, issues)
│   ├── db.py             — PostgreSQL connection + insert logic
│   └── main.py           — Orchestrator (fetch → load)

## How to Run
```bash
cd ~/github-pipline
conda activate de-pipeline
docker compose up -d
python -m ingest.main
Verify Data
docker exec -it github-pipline-postgres-1 psql -U postgres -d github_analytics -c "\dt"
docker exec -it github-pipline-postgres-1 psql -U postgres -d github_analytics -c "SELECT COUNT(*) FROM raw_repos;"
docker exec -it github-pipline-postgres-1 psql -U postgres -d github_analytics -c "SELECT name, stars FROM raw_repos ORDER BY stars DESC LIMIT 10;"
Commands to Resume Next Session
cd ~/github-pipline
conda activate de-pipeline
docker compose up -d
git log
Next Up (Week 2)
- Install dbt
- Create staging models (clean/cast raw data)
- Build star schema (dimensions + facts)
- Add data quality tests
```

# Week 2 — dbt Transformations ✅

## Tools Learned
- **dbt-core + dbt-postgres** — transformation framework
- **dbt models** — views (staging) and tables (marts)
- **dbt tests** — not_null, unique, accepted_values, relationships
- **dbt docs** — auto-generated lineage graph

## Project Structure Added
dbt_transforms/
├── dbt_project.yml
├── models/
│   ├── sources.yml
│   ├── staging/
│   │   ├── stg_repos.sql
│   │   ├── stg_commits.sql
│   │   ├── stg_issues.sql
│   │   └── schema.yml
│   └── marts/
│       ├── dim_repositories.sql
│       ├── dim_dates.sql
│       ├── fact_commits.sql
│       └── schema.yml

## Star Schema
| Table | Type | Rows | Description |
|-------|------|------|-------------|
| `dim_repositories` | Dimension | 50 | Unique repos from raw_repos |
| `dim_dates` | Dimension | 845 | Calendar dates from commits/issues |
| `fact_commits` | Fact | 4,861 | Commit facts linked to dims |

## Commands to Run Next Time
```bash
cd ~/OpenCode/github-pipline
conda activate de-pipeline
docker compose up -d
cd dbt_transforms
dbt debug
dbt run
dbt test
git log
Next Up (Week 3)
- Install Prefect
- Wrap ingestion in Prefect flows
- Task dependencies, retry, scheduling
- Parameterization (language, repo count)
```
# Week 3 — Prefect Orchestration ✅

## Tools Learned
- **Prefect 3.x** — `@task`/`@flow` decorators, `serve()`, deployment, cron scheduling
- **Docker Compose (multi-service)** — Postgres + Prefect Server + Worker containers
- **dbt profiles.yml** — connection config for containerized environments

## Architecture
GitHub API → Prefect Flow (tasks) → PostgreSQL → dbt build → Star Schema
                ↑                          ↑
           Prefect Server (UI:4200)    Postgres (5432)
                ↑
           Worker (runs flow code)

## Project Structure Added
github-pipline/
├── flows/
│   ├── ingestion_flow.py   — @task / @flow wrapping ingest + dbt
│   └── deploy.py            — serve() with daily cron
├── Dockerfile               — Python + dbt container image
├── Makefile                 — one-command targets (up, run, all)
├── dbt_transforms/
│   └── profiles.yml         — dbt connection config (uses env vars)

## Key Commands
```bash
make all                    # start everything + serve the flow
make up                     # docker compose up -d only

# Trigger a manual run
docker compose exec worker prefect deployment run 'ingestion-pipeline/github-pipeline'

# Check data
docker compose exec postgres psql -U postgres -d github_analytics -c "SELECT COUNT(*) FROM raw_repos;"
docker compose exec postgres psql -U postgres -d github_analytics -c "SELECT * FROM public_marts.dim_repositories ORDER BY stars DESC LIMIT 10;"

# Week 4 — Visualization & Polish ✅

## Tools Learned
- **Metabase** — BI dashboards; auto-provisioning via REST API (`/api/setup`, `/api/session`, `/api/card`, `/api/dashboard`)
- **dbt source freshness** — `dbt source freshness` command surfaces source staleness warnings/errors
- **Docker initdb** — `.docker-entrypoint-initdb.d` runs once on a **fresh** volume only (gotcha: existing volumes need manual DB creation)
- **Docker healthchecks + Makefile orchestration** — idempotent `make up`

## What Changed
| Item | Detail |
|------|--------|
| `metabase/` | `provision.py` — auto-setup Metabase, connect Postgres, build 3 cards + dashboard |
| `docker/initdb/` | `01-create-metabase-db.sql` — creates `metabase` DB on fresh volumes |
| `Makefile` | `make up` starts postgres first, ensures `metabase` DB exists, then starts everything; `make setup-metabase` runs `provision.py` |
| `flows/ingestion_flow.py` | Added `task_source_freshness` running `dbt source freshness` after `dbt build` |
| dbt marts | `dim_dates` gains `date_key`; `fact_commits`/`fact_issues` join it; `dim_repositories` gains `stars`; relationship tests added |
| ingest | `fetch_issues` fetches `state=all`; `raw_issues` gains `closed_at`/`fetched_at`; issue upsert updates state + closed_at |

## Dashboards (auto-provisioned)
`make setup-metabase` creates (idempotently):
- **Trending Repos** — top Python repos by stars (bar)
- **Commit Velocity** — commits per day (line)
- **Issue Closure by Repo** — open vs closed issues per repo (bar)

## How to Run
```bash
cd ~/OpenCode/github-pipline
make up                              # postgres → metabase DB → all services
docker compose exec worker python -c "from flows.ingestion_flow import ingestion_pipeline; ingestion_pipeline()"
make setup-metabase                  # wait for Metabase, then provision dashboards
```
Open http://localhost:3000 → login `admin@example.com` / `metabase-pass-123` (override via `METABASE_EMAIL`/`METABASE_PASSWORD` in `.env`).