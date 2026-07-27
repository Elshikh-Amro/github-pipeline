import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion_flow import ingestion_pipeline

if __name__ == "__main__":
    ingestion_pipeline.serve(
        name="github-pipeline",
        cron="0 6 * * *",
        parameters={
            "language": "python",
            "num_repos": 10,
            "max_commit_pages": 1,
            "run_dbt": True,
        },
    )