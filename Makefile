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
	@echo "Waiting for services to be ready..."
	@sleep 15
	@echo "Opening Prefect UI (pipeline run stages will appear here) in a new browser tab..."
	@open http://localhost:4200
	@echo "Opening Metabase in a new browser tab..."
	@open http://localhost:3000
	@echo ""
	@echo "Metabase login  -> email: admin@example.com   password: metabase-pass-123"
	@echo "                  (override via METABASE_EMAIL / METABASE_PASSWORD in .env)"
	@echo ""
	@echo "Running the pipeline now (watch it live in the Prefect tab)..."
	@docker compose exec -T worker python -c "from flows.ingestion_flow import ingestion_pipeline; ingestion_pipeline()"
	@echo ""
	@echo "Pipeline complete! Dashboards are ready at:"
	@echo "  Prefect   -> http://localhost:4200"
	@echo "  Metabase  -> $$(python metabase/provision.py --print-dashboard-url 2>/dev/null || echo http://localhost:3000)"