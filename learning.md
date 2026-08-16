# Learning Guide — GitHub Analytics Pipeline

A complete, beginner-friendly walkthrough of this data engineering project. It explains **what** each piece is, **why** it exists, **how** we implemented it, and **how it fits** into the bigger picture — plus external documentation and study guidance for someone learning data engineering by building.

---

## Table of Contents

1. [The Big Picture](#1-the-big-picture)
2. [Core Concepts You Need First](#2-core-concepts-you-need-first)
3. [Project Architecture](#3-project-architecture)
4. [Week 1 — The EL Pipeline (Fetching & Storing Data)](#4-week-1--the-el-pipeline)
5. [Week 2 — dbt Transformations (Data Modeling)](#5-week-2--dbt-transformations)
6. [Week 3 — Prefect Orchestration (Automation)](#6-week-3--prefect-orchestration)
7. [Week 4 — Metabase & Polish (Visualization)](#7-week-4--metabase--polish)
8. [Data Model Reference](#8-data-model-reference)
9. [External Documentation](#9-external-documentation)
10. [How to Study This — Concept vs Implementation](#10-how-to-study-this--concept-vs-implementation)
11. [What to Learn Next](#11-what-to-learn-next)

---

## 1. The Big Picture

A **data pipeline** moves data from where it is generated to where it can be analyzed, transforming it along the way. This project builds a complete pipeline around GitHub data:

```
GitHub API ──▶ Python (ingest) ──▶ PostgreSQL ──▶ dbt (transform) ──▶ Metabase (dashboard)
                                        ▲
                                 Prefect (orchestrator)
```

**The journey of one piece of data:**

1. **Fetch** — a Python script calls GitHub's REST API and asks for the top Python repos, their commits, and their issues.
2. **Store** — the raw JSON responses are written into PostgreSQL "raw" tables exactly as-is (the *landing* zone).
3. **Transform** — dbt runs SQL that cleans, casts, and reshapes raw data into a **star schema** (dimensions + facts) ready for analytics.
4. **Orchestrate** — Prefect wraps the whole thing so it runs automatically on a schedule, retries failures, and shows you every run in a UI.
5. **Visualize** — Metabase connects to the transformed tables so a human can explore the data and look at dashboards.

**Why build this?** Every company that "does analytics" needs this exact stack in some form. If you learn these five layers, you can talk to any data team: ingestion, warehousing, transformation, orchestration, and BI.

---

## 2. Core Concepts You Need First

These are the mental models you must have before reading the code. Each is explained in plain English.

### 2.1 Databases vs Data Warehouses
- A **database** stores application data (e.g., the user table behind a website).
- A **data warehouse** is a database optimized for *reading large amounts of historical data for analysis*.
- We use PostgreSQL for both the raw storage and the transformed warehouse — fine for a learning project. In production you might see Snowflake, BigQuery, or Redshift.

### 2.2 Raw Layer vs Transformed Layer
- **Raw layer (bronze):** data exactly as it arrived from the source. Messy, complete, never assume clean. In this project: `raw_repos`, `raw_commits`, `raw_issues`.
- **Transformed layer (silver/gold):** cleaned, typed, renamed, joined into a shape analysts actually query. In this project: the `staging` views and `marts` tables.

### 2.3 Star Schema & Dimensional Modeling
The most common analytics data model (used by Kimball). Two kinds of tables:

- **Dimension tables (`dim_...`)** — descriptive "who/what/where/when". Few rows, lots of attributes. E.g., `dim_repositories` (a repo's name, owner, stars), `dim_dates` (a calendar with year/month/day).
- **Fact tables (`fact_...`)** — measurable events. Many rows. E.g., `fact_commits` (one row per commit), `fact_issues` (one row per issue).
- Facts join to dimensions via keys. This lets you ask "how many commits happened on Tuesdays in repo X?" by joining fact → dim_dates and fact → dim_repositories.

### 2.4 EL vs ELT
- **EL**: Extract then Load, transform happens outside the warehouse.
- **ELT**: Extract, Load raw, then Transform *inside* the warehouse using SQL (what we do). dbt is the poster child of ELT — it only writes SQL.

### 2.5 Idempotency
An operation is **idempotent** if running it twice produces the same result. Our `ON CONFLICT ... DO UPDATE/NOTHING` upserts make re-running the pipeline safe. This is critical for anything scheduled.

### 2.6 Orchestration
Orchestration = managing the *when, retries, order, and visibility* of your pipeline. It answers: "run the fetch task, then if it succeeds load, then run dbt, and if any step fails retry it 3 times." Without it, you rely on cron + hope.

---

## 3. Project Architecture

```
github-pipline/
├── docker-compose.yml        ← defines all 4 services & their connections
├── Dockerfile                ← builds the "worker" image (Python + dbt + Prefect)
├── Makefile                  ← friendly commands: make up / make all / ...
├── .env                      ← secrets (GITHUB_TOKEN) — gitignored!
├── ingest/                   ← WEEK 1: plain Python EL
│   ├── github_api.py         ←   talks to the GitHub REST API
│   ├── db.py                 ←   connects to PostgreSQL, creates tables, saves rows
│   └── main.py               ←   orchestrates fetch → save (no Prefect)
├── flows/                    ← WEEK 3: Prefect versions of the above
│   ├── ingestion_flow.py     ←   @task / @flow wrapping ingest + dbt
│   └── deploy.py             ←   registers the flow on a daily cron schedule
├── dbt_transforms/           ← WEEK 2: dbt project
│   ├── dbt_project.yml       ←   config (name, materializations, schemas)
│   ├── profiles.yml          ←   how dbt connects to the database
│   └── models/
│       ├── staging/          ←   stg_*.sql (clean raw data into views)
│       └── marts/            ←   dim_*.sql + fact_*.sql (star schema tables)
├── metabase/provision.py     ← WEEK 4: auto-setup dashboards via Metabase API
└── docker/initdb/            ← SQL run once when Postgres first boots
```

---

## 4. Week 1 — The EL Pipeline

**Goal:** Get data out of GitHub and into PostgreSQL. Nothing fancy — just fetch and load.

### 4.1 `ingest/github_api.py` — talking to an external API

**What it is:** A thin client for the GitHub REST API using Python's `requests` library.

**Key function breakdown:**

| Function | What it does |
|----------|--------------|
| `_get(url, params, max_retries=3)` | Helper that GETs a URL with headers, retries on failure, and handles GitHub's rate limiting (waits `Retry-After` seconds on a 403). |
| `fetch_top_repos(num, language)` | Calls `GET /search/repositories?q=language:python&sort=stars`. Loops over pages until it has `num` repos. Returns a **list of dicts**. |
| `fetch_commits(owner, repo, max_pages)` | Calls `GET /repos/{owner}/{repo}/commits`. Returns commits with `sha`, message, author, date. |
| `fetch_issues(owner, repo)` | Calls `GET /repos/{owner}/{repo}/issues?state=all`. Returns issues including `closed_at` so we can compute closure rates. |

**Key design points:**
- **Pagination:** GitHub returns results in pages. `fetch_top_repos` keeps incrementing `page` until it has enough repos.
- **Rate limiting:** GitHub limits unauthenticated requests. `_get` watches for the `403 rate limit` response and sleeps. We also pass `GITHUB_TOKEN` from `.env` for a higher limit.
- **Thin, pure functions:** `github_api.py` knows nothing about databases. It returns plain dicts. This separation (API code vs DB code) is a software design principle called *separation of concerns* — each file has one job.

**Implementation notes:**
```python
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
```
- Loads `.env` with `python-dotenv`, reads `GITHUB_TOKEN`. If absent, it calls the API unauthenticated (lower limits).

### 4.2 `ingest/db.py` — persisting data

**What it is:** Database access layer using `psycopg` (PostgreSQL driver for Python).

| Function | What it does |
|----------|--------------|
| `get_connection()` | Creates a connection from env vars (host, port, user, password, db). |
| `create_tables(conn)` | `CREATE TABLE IF NOT EXISTS` for `raw_repos`, `raw_commits`, `raw_issues`. Also runs idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migrations. |
| `save_repos(conn, repos)` | Bulk-inserts repos with **upsert**: `ON CONFLICT (id) DO UPDATE SET stars = EXCLUDED.stars, ...`. |
| `save_commits(conn, commits)` | Inserts commits; on conflict `DO NOTHING` (a commit sha never changes). |
| `save_issues(conn, issues)` | Inserts issues with upsert updating `state` and `closed_at` (an issue can go open → closed). |

**What is an upsert?** `INSERT ... ON CONFLICT (id) DO UPDATE` means: try to insert; if a row with the same primary key already exists, update it instead. This makes re-runs safe — you never get duplicate errors or duplicate rows.

**Implementation notes:**
- Uses `executemany` for bulk inserts — one network round trip for many rows instead of one per row.
- Column types matter: `id BIGINT PRIMARY KEY` (GitHub ids exceed 32-bit int), timestamps as `TIMESTAMPTZ`.
- `create_tables` is idempotent because the pipeline may run many times against the same DB.

### 4.3 `ingest/main.py` — the first orchestrator

**What it is:** A plain Python script that ties fetch + load together (before we knew about Prefect).

```python
repos = fetch_top_repos(50)
for r in repos:
    commits = fetch_commits(owner, name, max_pages=1)
    issues = fetch_issues(owner, name)
    ...
time.sleep(0.5)  # be nice to GitHub
```

**What it teaches you:** Even a naive loop over 50 repos with a half-second delay is a (slow, fragile) pipeline. Later, Week 3 replaces this hand-rolled orchestration with Prefect — and you'll see *why* you need a framework: retries, visibility, scheduling, and failure handling are all things you'd otherwise re-invent badly.

### 4.4 How Week 1 contributes to the whole project

- It's the **source of all data**. Without it, nothing downstream has anything to work with.
- The raw tables are the **single source of truth**; dbt (Week 2) never talks to GitHub again.
- It establishes the pattern: *fetch from source → store raw* — the "E" and "L" in ELT.

### 4.5 Focus points for a beginner

- **Concept to master:** REST APIs, JSON, pagination, rate limits, idempotent upserts, connection management.
- **Implementation to master:** writing small pure functions, separating API from DB code, `try/finally` around connections, using env vars for config.

---

## 5. Week 2 — dbt Transformations

**Goal:** Turn messy raw JSON-ish tables into a clean star schema using SQL, with automated tests and documentation.

**Why dbt and not plain SQL?** dbt is an **ELT framework**: it runs SQL *inside* the warehouse and layers useful engineering features on top:

- **Incremental/reference-able models** — `{{ ref('stg_repos') }}` lets models depend on each other and dbt builds them in the right order.
- **Materializations** — pick `view` (cheap, recompute every time) or `table` (snapshot of results) or `incremental`.
- **Tests** — declare `not_null`, `unique`, `relationships` in YAML; dbt runs them as assertions.
- **Documentation & lineage** — auto-generates a dependency graph (`dbt docs`).
- **Environments** — profiles let the same code run in dev/prod.

### 5.1 Project files

**`dbt_project.yml`**
```yaml
name: dbt_transforms
profile: dbt_transforms
models:
  dbt_transforms:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```
- `staging` models become **views** (cheap, always fresh).
- `marts` models become **tables** (materialized for fast querying by Metabase).
- Custom schemas `staging` and `marts` get created inside Postgres (they appear as `public_staging` and `public_marts`).

**`profiles.yml`** — tells dbt how to connect (host/user/password/db). It reads env vars so it works inside Docker too.

### 5.2 Staging models — the "clean" layer

Staging = **minimal cleaning only** — cast types, rename columns, select the columns you need. No joins yet, no business logic.

**`stg_repos.sql`**
```sql
SELECT
    id AS repo_id,
    name AS repo_name,
    owner,
    stars,
    ...
    COALESCE(description, 'No description') AS description
FROM {{ source('raw', 'raw_repos') }}
```
- `{{ source('raw', 'raw_repos') }}` tells dbt to read from the `raw_repos` *source* (declared in `sources.yaml`) rather than a model. This creates a clean dependency boundary.
- `COALESCE(...)` replaces `NULL` descriptions with a default string.

**`stg_issues.sql`** — the interesting one:
```sql
closed_at::TIMESTAMPTZ AS closed_at,
closed_at IS NOT NULL AS is_closed,
EXTRACT(EPOCH FROM (closed_at - created_at)) / 86400 AS days_to_close
```
- Casts strings to timestamps, creates a boolean flag, and computes **days to close** — a derived business metric computed once here so downstream doesn't repeat it.

### 5.3 Mart models — the star schema

**`dim_repositories.sql`**
```sql
SELECT DISTINCT repo_id, repo_name, owner, stars, language, description, url
FROM {{ ref('stg_repos') }}
```
- A dimension = one row per repo. `DISTINCT` guards against duplicates from multiple fetches.

**`dim_dates.sql`**
- Builds a calendar from every date found in commits and issues (`UNION` of two `SELECT DISTINCT`), then adds derived attributes: `year`, `month`, `day_of_week`, `day_name`, and a surrogate key `date_key` (`YYYYMMDD` as an integer).

**`fact_commits.sql` / `fact_issues.sql`**
```sql
SELECT c.sha, c.repo_name, r.repo_id, c.committed_at, d.date_key, ...
FROM {{ ref('stg_commits') }} c
LEFT JOIN {{ ref('dim_repositories') }} r ON c.repo_name = r.repo_name
LEFT JOIN {{ ref('dim_dates') }} d ON c.committed_at::DATE = d.date_day
```
- A fact row is an *event* (one commit / one issue). We attach the dimension keys so analysts can slice by repo or date.
- **`LEFT JOIN`** keeps every fact even if a dimension is missing (we never want to drop data).

### 5.4 Tests & Sources

**`staging/sources.yaml`** — declares the raw tables as dbt *sources* and adds **freshness** thresholds:
```yaml
tables:
  - name: raw_repos
    loaded_at_field: fetched_at
    freshness:
      warn_after: {count: 1, period: day}
      error_after: {count: 3, period: day}
```
- If the newest `fetched_at` is older than 1 day → warn; older than 3 days → error. This is how you get alerted when the pipeline silently stops.

**`schema.yaml` files** — tests on columns:
- `unique` / `not_null` on primary keys (a good primary key must be both).
- `accepted_values` (e.g., issue `state` is only `open`/`closed`).
- `relationships` — checks that `fact_commits.repo_id` exists in `dim_repositories.repo_id`. This enforces referential integrity across your star schema.

### 5.5 How Week 2 contributes to the whole project

- Raw tables are **unqueryable by analysts** (mixed types, duplicates, missing columns). dbt produces the *clean, documented, tested* layer that dashboards read.
- The **tests** give you confidence the pipeline is correct — when `dbt test` fails, you know something upstream broke.
- The **star schema** is what makes the Metabase dashboards (Week 4) trivial to write.

### 5.6 Focus points for a beginner

- **Concept to master:** ELT vs ETL, dimensional modeling (Kimball), materializations (view vs table vs incremental), surrogate keys, source freshness, data quality tests.
- **Implementation to master:** SQL joins, `COALESCE`, `EXTRACT`, `CAST`, `DISTINCT`, and reading `ref()`/`source()` semantics.

---

## 6. Week 3 — Prefect Orchestration

**Goal:** Replace the naive `ingest/main.py` loop with a real orchestrator that handles retries, caching, scheduling, and gives you a UI.

### 6.1 `flows/ingestion_flow.py` — the Prefect flow

Prefect has two core concepts:

- **`@task`** — an individual unit of work (e.g., "fetch commits for this repo"). Tasks have retries, caching, and timeouts.
- **`@flow`** — the overall workflow that calls tasks in order. Flows have names, parameters, schedules, and appear in the UI.

**Task definitions:**
```python
@task(retries=2, retry_delay_seconds=30, cache_key_fn=task_input_hash)
def task_fetch_repos(num: int = 10, language: str = "python"):
    return fetch_top_repos(num, language)
```
- `retries=2` — if fetching repos fails (network hiccup, rate limit), Prefect retries 2 times with a 30s delay.
- `cache_key_fn=task_input_hash` — if the same task runs twice with the same inputs, Prefect reuses the cached result instead of hitting GitHub again. Perfect for the "don't re-fetch if nothing changed" case.

**The flow:**
```python
@flow(log_prints=True)
def ingestion_pipeline(language="python", num_repos=10, max_commit_pages=1, run_dbt=True):
    task_create_tables()                          # ensure tables exist
    repos = task_fetch_repos(num_repos, language) # fetch top repos
    repo_count = task_save_repos(repos)           # save them
    for r in repos:                               # for each repo...
        commits = task_fetch_commits(owner, name, max_commit_pages)
        issues = task_fetch_issues(owner, name)
        all_commits.extend(commits); all_issues.extend(issues)
    task_save_commits(all_commits)
    task_save_issues(all_issues)
    if run_dbt:
        task_run_dbt()          # dbt build  → models + tests
        task_source_freshness() # dbt source freshness → staleness check
```
- `log_prints=True` makes `print()` calls show up in the Prefect UI logs.
- **Parameters** (`num_repos`, `run_dbt`, ...) mean you can trigger the same flow with different inputs without editing code.

**`task_run_dbt` / `task_source_freshness`** — run dbt via `subprocess` so the flow drives the whole ELT pipeline (ingest *and* transform) as one unit.

### 6.2 `flows/deploy.py` — scheduling

```python
ingestion_pipeline.serve(
    name="github-pipeline",
    cron="0 6 * * *",   # every day at 06:00
    parameters={"language": "python", "num_repos": 10, ...},
)
```
- `serve()` registers a **deployment** (name + schedule + default parameters) with the Prefect server and keeps the process listening.
- The cron `0 6 * * *` = run at 06:00 every day.

### 6.3 How orchestration fits in the Docker world

- **`prefect-server`** — the API + UI (port 4200). Stores flow runs in the same Postgres.
- **`worker`** — a process that listens for scheduled runs and executes the flow code (our Dockerfile-built image with Python + dbt + Prefect).
- Together: server decides *when*, worker does *the work*.

### 6.4 How Week 3 contributes to the whole project

- Turns a manual script into a **production-style automated pipeline** with schedules, retries, caching, and full visibility.
- This is the "modern orchestration" skill every data team wants — Prefect, Airflow, Dagster.

### 6.5 Focus points for a beginner

- **Concept to master:** tasks vs flows, retries, caching, scheduling (cron), deployments, worker/server architecture, the UI (flow runs, task runs, logs).
- **Implementation to master:** decorators, function signatures with parameters, subprocess calls, and how code runs inside containers.

---

## 7. Week 4 — Metabase & Polish

**Goal:** Let humans see the data. Also: fix operational gotchas (freshness, healthchecks, idempotent setup).

### 7.1 Metabase + `docker-compose.yml`

Metabase is an open-source BI tool. You point it at a database and it lets non-engineers build charts and dashboards with clicks.

We added it as a 4th service:
```yaml
metabase:
  image: metabase/metabase:latest
  environment:
    MB_DB_TYPE: postgres            # Metabase stores its OWN config in postgres
    MB_DB_DBNAME: metabase          #   (the `metabase` database)
    MB_DB_HOST: postgres
  ports: ["3000:3000"]
```
**Gotcha we hit:** Metabase needs a database called `metabase` to exist before it starts. Docker's `initdb` scripts only run on a **fresh** Postgres volume. On our existing volume the DB wouldn't be created → Metabase would crash. **Fix:** the `make up` target creates it idempotently:
```make
@docker compose exec -T postgres psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='metabase'" | grep -q 1 \
  || docker compose exec -T postgres psql -U postgres -c "CREATE DATABASE metabase"
```
- "If the database doesn't exist, create it." Re-runnable, no harm.

**Healthchecks** were also added so the worker only starts *after* Prefect is truly ready (fixing a race condition we hit where the worker crashed on boot).

### 7.2 `metabase/provision.py` — automating the dashboards

Metabase has a **REST API**, so instead of clicking through the UI we wrote a script that:

1. `wait_for_health()` — polls `/api/health` until Metabase responds.
2. `get_session()` / `setup_new_instance()` — log in as admin, or on first boot run the `/api/setup` wizard programmatically.
3. `ensure_database()` — register our Postgres warehouse as the "GitHub Analytics" data source.
4. `provision()` — create 3 **cards** (questions/queries):
   - **Trending Repos** — `SELECT repo_name, stars FROM dim_repositories ORDER BY stars DESC` (bar chart).
   - **Commit Velocity** — commits per day from `fact_commits` (line chart).
   - **Issue Closure by Repo** — open vs closed per repo from `fact_issues`.
5. Assemble the cards into a **dashboard** via `PUT /api/dashboard/{id}`.
6. **Idempotent:** every step checks "does this already exist?" and skips it — safe to re-run.

**Why the API approach?** Dashboards you click together by hand aren't reproducible or version-controlled. Provisioning via API means a fresh deployment gets the exact same dashboards automatically — *infrastructure as code*.

### 7.3 Other Week 4 polish

- **`Makefile` targets** — `make up` (services), `make all` (up + open tabs + run pipeline + print dashboard URL + login creds), `make run` (trigger deployment), `make setup-metabase` (provision dashboards).
- **`.gitignore` hygiene** — stopped tracking `.DS_Store` and log files.
- **Documentation** — `README.md` (how to run/test), `NOTES.md` (per-week log), this file.

### 7.4 How Week 4 contributes to the whole project

- The pipeline is now *valuable*: real people can explore the data and answer questions.
- It closes the loop from "raw API bytes" to "business decision dashboard".

### 7.5 Focus points for a beginner

- **Concept to master:** BI tools, dashboards, the REST-API provisioning pattern, healthchecks/dependency ordering in Docker, idempotent setup scripts.
- **Implementation to master:** making authenticated API calls, polling for readiness, reading API response shapes, Docker compose service definitions.

---

## 8. Data Model Reference

Raw tables (`public` schema):

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `raw_repos` | Repos fetched from search API | `id` PK, `name`, `owner`, `stars`, `forks` |
| `raw_commits` | Commits per repo | `sha` PK, `repo_name`, `committed_at` |
| `raw_issues` | Issues (open + closed) | `id` PK, `state`, `closed_at`, `created_at` |

Staging views (`public_staging`):

| Model | Cleans... |
|-------|-----------|
| `stg_repos` | renames, COALESCE description |
| `stg_commits` | truncates message, adds length |
| `stg_issues` | casts timestamps, adds `is_closed`, `days_to_close` |

Marts (`public_marts`):

| Model | Type | Description |
|-------|------|-------------|
| `dim_repositories` | Dimension | One row per repo |
| `dim_dates` | Dimension | Calendar with `date_key`, year, month, day |
| `fact_commits` | Fact | One row per commit → repo + date |
| `fact_issues` | Fact | One row per issue → repo + date, closure metrics |

Lineage:

```
raw_repos ─▶ stg_repos ─▶ dim_repositories ─┐
raw_commits─▶ stg_commits─▶ fact_commits ────┤──▶ Metabase dashboards
raw_issues ─▶ stg_issues ─▶ fact_issues ────┘
                          └▶ dim_dates ◀─────┘
```

---

## 9. External Documentation

### Python & Data Basics
- [Python `requests` library](https://requests.readthedocs.io/en/latest/) — HTTP client we use for the GitHub API.
- [GitHub REST API docs](https://docs.github.com/en/rest) — the data source; read the "Search", "Commits", "Issues" endpoints.
- [psycopg (PostgreSQL driver) docs](https://www.psycopg.org/psycopg3/docs/) — how Python talks to Postgres.
- [Python `python-dotenv`](https://saurabh-kumar.com/python-dotenv/) — loading `.env` secrets.

### Databases & Warehousing
- [PostgreSQL documentation](https://www.postgresql.org/docs/) — the database engine. Look at `psql`, SQL, and `INSERT ... ON CONFLICT`.
- [Postgres Docker image docs](https://hub.docker.com/_/postgres) — env vars, healthchecks, initdb scripts.
- [What is a data warehouse?](https://www.snowflake.com/guides/what-is-a-data-warehouse) — conceptual overview.

### Dimensional Modeling & dbt
- [dbt documentation](https://docs.getdbt.com/docs/introduction) — the official docs (models, tests, sources, freshness).
- [dbt tutorial (jaffle shop)](https://docs.getdbt.com/learn/getting-started) — the canonical starter project, worth doing fully.
- [The Kimball Dimensional Modeling book (agile data warehousing)](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/) — the bible of star schemas.
- [dbt tests docs](https://docs.getdbt.com/docs/build/tests) — generic + singular tests.
- [dbt source freshness](https://docs.getdbt.com/docs/build/sources#snapshotting-source-data-freshness) — what our freshness config does.

### Orchestration
- [Prefect documentation](https://docs.prefect.io/latest/) — flows, tasks, deployments, scheduling.
- [Prefect tutorial](https://docs.prefect.io/latest/tutorial/) — start here for the framework.
- [Cron expression guide](https://crontab.guru/) — decode `0 6 * * *`.
- [Airflow vs Prefect](https://docs.prefect.io/latest/getting-started/why-prefect/) — why orchestrators exist.

### Containerization & DevOps
- [Docker documentation](https://docs.docker.com/) — images, containers, compose.
- [Docker Compose reference](https://docs.docker.com/compose/) — services, depends_on, healthchecks, volumes.
- [12-factor app config](https://12factor.net/config) — why we use env vars, not hardcoded values.

### Visualization & APIs
- [Metabase documentation](https://www.metabase.com/docs/latest/) — dashboards, cards, admin.
- [Metabase REST API](https://www.metabase.com/docs/latest/api-documentation) — what `provision.py` calls (`/api/setup`, `/api/session`, `/api/card`, `/api/dashboard`).
- [HTTP status codes](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status) — 200/400/403/404 etc., needed to debug any API.

---

## 10. How to Study This — Concept vs Implementation

A common trap in data engineering is **memorizing tools** (dbt syntax, Prefect decorators) without understanding **why they exist**. Both matter, but in different proportions depending on your stage.

### Priority order — concepts first, then tools

| # | Concept (learn deeply) | Tool that implements it (practice until comfortable) |
|---|------------------------|------------------------------------------------------|
| 1 | How data moves: fetch → store → transform → analyze | HTTP requests, SQL INSERT, SQL SELECT/joins |
| 2 | Why raw vs transformed layers exist | PostgreSQL schemas, dbt models |
| 3 | Dimensional modeling (star schema) | `dim_*` / `fact_*` models, `ref()` |
| 4 | Data quality & freshness | dbt tests, `dbt source freshness` |
| 5 | Why you need orchestration | Prefect tasks/flows, cron schedules |
| 6 | Reproducibility & infra-as-code | Docker Compose, provisioning scripts |
| 7 | Communicating results | Metabase dashboards |

### How to learn each layer (recommended order)

1. **Start with SQL.** Open `psql` against the `github_analytics` DB and run the queries by hand. Ask: "can I reproduce what `fact_commits` computes?" SQL is the universal language of this field; everything else is glue.
2. **Trace one piece of data end-to-end.** Pick a commit, then follow it: raw table → stg view → fact table → dashboard. If you can trace any single row through all 5 layers, you understand the pipeline.
3. **Break things on purpose.** Delete a row and watch the freshness test fail. Stop the worker and watch Prefect show a failed run. Rename a column and watch dbt tests error. *Failure is the fastest teacher here* — you'll learn what each safety net is actually protecting.
4. **Read logs.** Both Prefect logs and `docker compose logs worker` show exactly what happened. Being able to read logs is a top-5 skill.
5. **Rebuild pieces from memory.** After studying, delete `flows/ingestion_flow.py` and rewrite it. Then delete a mart model and rebuild it. Retrieval beats re-reading.

### Where to spend your effort (rough split)

- **~40% on concepts** — dimensional modeling, ELT, orchestration principles, idempotency. These transfer to any tool.
- **~35% on hands-on** — actually running the pipeline, writing SQL, breaking/fixing things, reading logs.
- **~25% on the surrounding practices** — Docker, Makefile ergonomics, git hygiene, env vars, writing docs like this one. These make you a *professional*, not just a script-writer.

### The "learn by building" method — proven steps

1. **Build a tiny version first** (this project: one repo, one API call, one table).
2. **Add layers incrementally** (we did: plain EL → dbt → Prefect → Metabase). Don't jump to the full stack on day one — you'll drown.
3. **Automate the annoying parts** — the day you get tired of running a command by hand is the day you learn `make`, cron, or Prefect scheduling. Necessity drives mastery.
4. **Write down what you did** (like `NOTES.md`). Future-you will thank present-you.
5. **Make it visible** (the dashboards). Seeing your data in a chart is enormously motivating and validates that everything works.

---

## 11. What to Learn Next

When you've mastered this project, the natural progression is:

1. **Incremental models** — make `fact_commits` and `fact_issues` incremental instead of full rebuilds (dbt `+materialized: incremental`). Handle how to backfill.
2. **Handling failures & backfills** — how to re-run just one failed task, and how to backfill missing days on a schedule.
3. **Testing in CI** — run `dbt test` in GitHub Actions on every PR (that's the GitHub Actions stretch goal in `plan.md`).
4. **Great Expectations / data contracts** — richer data quality checks than dbt generic tests.
5. **Streaming (Redpanda/Kafka)** — replace the batch fetch with event streaming (the other stretch goal).
6. **Cloud versions** — swap Docker Postgres for BigQuery/Redshift/Snowflake, deploy Prefect Cloud or Airflow, add dbt Cloud.
7. **Bigger orchestration patterns** — parameterized backfills, dynamic fan-out, data versioning, SLAs/alerting.

---

*This document intentionally mirrors the build order of the project. If you can explain each layer to someone else — why it exists, what failure mode it prevents, and how it connects to its neighbors — you've truly learned it.*
