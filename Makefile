.PHONY: up deploy run all

up:
	docker compose up -d

deploy:
	docker compose exec worker python flows/deploy.py

run:
	docker compose exec worker prefect deployment run 'ingestion-pipeline/github-pipeline'

logs:
	docker compose logs -f worker

all:
	docker compose up -d
	@echo "Waiting for services..."
	@sleep 10
	@echo "Pipeline is serving on http://localhost:4200"
	@echo "The first run will trigger automatically on the cron schedule (daily at 6am)."
	@echo "Or trigger one now: docker compose exec worker python -c \"from ingestion_flow import ingestion_pipeline; ingestion_pipeline()\""