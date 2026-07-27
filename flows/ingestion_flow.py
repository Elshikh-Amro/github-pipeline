import os
import subprocess
from textwrap import dedent

from prefect import flow, task
from prefect.tasks import task_input_hash

from ingest.github_api import fetch_top_repos, fetch_commits, fetch_issues
from ingest.db import get_connection, create_tables, save_repos, save_commits, save_issues


@task(retries=2, retry_delay_seconds=30, cache_key_fn=task_input_hash)
def task_fetch_repos(num: int = 10, language: str = "python"):
    return fetch_top_repos(num, language)


@task(retries=2, retry_delay_seconds=10)
def task_fetch_commits(owner: str, name: str, max_pages: int = 1):
    return fetch_commits(owner, name, max_pages)


@task(retries=1, retry_delay_seconds=10)
def task_fetch_issues(owner: str, name: str):
    return fetch_issues(owner, name)


@task
def task_create_tables():
    conn = get_connection()
    create_tables(conn)
    conn.close()


@task
def task_save_repos(repos: list) -> int:
    conn = get_connection()
    try:
        return save_repos(conn, repos)
    finally:
        conn.close()


@task
def task_save_commits(commits: list) -> int:
    conn = get_connection()
    try:
        return save_commits(conn, commits)
    finally:
        conn.close()


@task
def task_save_issues(issues: list) -> int:
    conn = get_connection()
    try:
        return save_issues(conn, issues)
    finally:
        conn.close()


@task
def task_run_dbt():
    result = subprocess.run(
        ["dbt", "build"],
        cwd=os.path.abspath("dbt_transforms"),
        capture_output=True,
        text=True,
    )
    print(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt build failed:\n{result.stderr[-1000:]}")
    return result.returncode


@flow(log_prints=True)
def ingestion_pipeline(
    language: str = "python",
    num_repos: int = 10,
    max_commit_pages: int = 1,
    run_dbt: bool = True,
):
    task_create_tables()

    repos = task_fetch_repos(num_repos, language)
    repo_count = task_save_repos(repos)
    print(f"Saved {repo_count} repos")

    all_commits = []
    all_issues = []

    for r in repos:
        owner, name = r["owner"], r["name"]
        print(f"Fetching {owner}/{name}...")
        commits = task_fetch_commits(owner, name, max_commit_pages)
        issues = task_fetch_issues(owner, name)
        all_commits.extend(commits)
        all_issues.extend(issues)

    commit_count = task_save_commits(all_commits)
    issue_count = task_save_issues(all_issues)
    print(f"Saved {commit_count} commits, {issue_count} issues")

    if run_dbt:
        dbt_result = task_run_dbt()
        print(f"dbt build completed with exit code {dbt_result}")

    print("Pipeline complete!")
    return {"repos": repo_count, "commits": commit_count, "issues": issue_count}
