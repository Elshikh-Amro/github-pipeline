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