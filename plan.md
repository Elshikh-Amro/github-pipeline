# DATA ENGINEERING PIPELINE — Full Plan

## Architecture
GitHub API → Python → PostgreSQL → dbt → Metabase
                          ↑
                     Prefect (orchestrates)

## Tech Stack
- Python (ingestion)
- PostgreSQL (warehouse)
- dbt (transformations)
- Prefect (orchestration)
- Metabase (dashboards)
- Docker Compose (infrastructure)

## Data Source
GitHub API — top Python repos, commits, open issues

---

## Week 1 ✅ DONE
EL Pipeline — fetch data, store in PostgreSQL raw tables
Files: docker-compose.yml, ingest/{github_api,db,main}.py
Tables: raw_repos, raw_commits, raw_issues

## Week 2 ✅ DONE
- Install dbt-core + dbt-postgres
- dbt_project.yml, sources.yml
- Staging models: stg_repos.sql, stg_commits.sql, stg_issues.sql
- Mart models: dim_repositories, dim_dates, fact_commits
- dbt tests (not_null, unique, relationships)
- dbt docs (lineage graph)
- Concepts: dimensional modeling, materializations, incremental models

## Week 3 ✅ DONE
- Wrap Python ingestion into Prefect flows
- Task dependencies, retry logic
- Scheduled daily runs
- Logging, error handling
- Parameterization (language/topic)

## Week 4 ✅ DONE
- Connect Metabase to PostgreSQL
- Dashboards: trending repos, commit velocity, issue closure rates (auto-provisioned via metabase/provision.py)
- dbt source freshness tests
- Docker Compose for everything (not just DB)
- Stretch (future): Great Expectations, GitHub Actions CI/CD, Redpanda streaming

---

## Resume Commands
cd ~/github-pipline
conda activate de-pipeline
docker compose up -d
git log

## Verify Data
docker exec -it github-pipline-postgres-1 psql -U postgres -d github_analytics \
  -c "SELECT COUNT(*) FROM raw_repos;"