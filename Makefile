.PHONY: up deploy run all setup-metabase

up:
	docker compose up -d postgres
	@echo "Waiting for postgres to be healthy..."
	@sleep 5
	@echo "Ensuring metabase database exists..."
	@docker compose exec -T postgres psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='metabase'" | grep -q 1 \
		|| docker compose exec -T postgres psql -U postgres -c "CREATE DATABASE metabase"
	docker compose up -d

deploy:
	docker compose exec worker python flows/deploy.py

run:
	docker compose exec worker prefect deployment run 'ingestion-pipeline/github-pipeline'

setup-metabase:
	python metabase/provision.py

logs:
	docker compose logs -f worker

all: up
	@echo "Waiting for services..."
	@sleep 10
	@echo "Pipeline is serving on http://localhost:4200"
	@echo "The first run will trigger automatically on the cron schedule (daily at 6am)."
	@echo "Or trigger one now: docker compose exec worker python -c \"from flows.ingestion_flow import ingestion_pipeline; ingestion_pipeline()\""