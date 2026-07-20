import time
from ingest.github_api import fetch_top_repos, fetch_commits, fetch_issues
from ingest.db import get_connection, create_tables, save_repos, save_commits, save_issues


def main():
    conn = get_connection()
    create_tables(conn)

    print("Fetching top Python repos...")
    repos = fetch_top_repos(50)
    print(f"Got {len(repos)} repos")

    repo_count = save_repos(conn, repos)
    commit_count = 0
    issue_count = 0

    for i, r in enumerate(repos, 1):
        owner, name = r["owner"], r["name"]
        print(f"[{i}/{len(repos)}] {owner}/{name} — fetching commits & issues...")

        commits = fetch_commits(owner, name, max_pages=1)
        commit_count += save_commits(conn, commits)

        issues = fetch_issues(owner, name)
        issue_count += save_issues(conn, issues)

        time.sleep(0.5)  # be nice to GitHub

    conn.close()
    print(f"\nDone! Loaded: {repo_count} repos, {commit_count} commits, {issue_count} issues")


if __name__ == "__main__":
    main()