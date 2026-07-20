import os
import psycopg

DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
    "dbname": os.getenv("PGDATABASE", "github_analytics"),
}


def get_connection():
    return psycopg.connect(**DB_CONFIG)


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_repos (
                id BIGINT PRIMARY KEY,
                name TEXT,
                owner TEXT,
                stars INT,
                forks INT,
                language TEXT,
                description TEXT,
                url TEXT,
                fetched_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_commits (
                sha TEXT PRIMARY KEY,
                repo_name TEXT,
                author_name TEXT,
                message TEXT,
                committed_at TIMESTAMPTZ
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_issues (
                id BIGINT PRIMARY KEY,
                repo_name TEXT,
                title TEXT,
                state TEXT,
                created_at TIMESTAMPTZ
            );
        """)
    conn.commit()


def save_repos(conn, repos):
    if not repos:
        return 0
    rows = [(r["id"], r["name"], r["owner"], r["stars"], r["forks"],
             r["language"], r["description"], r["url"]) for r in repos]
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO raw_repos (id, name, owner, stars, forks, language, description, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                stars = EXCLUDED.stars,
                forks = EXCLUDED.forks,
                fetched_at = NOW();
        """, rows)
    conn.commit()
    return len(rows)


def save_commits(conn, commits):
    if not commits:
        return 0
    rows = [(c["sha"], c["repo_name"], c["author_name"], c["message"], c["committed_at"])
            for c in commits]
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO raw_commits (sha, repo_name, author_name, message, committed_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (sha) DO NOTHING;
        """, rows)
    conn.commit()
    return len(rows)


def save_issues(conn, issues):
    if not issues:
        return 0
    rows = [(i["id"], i["repo_name"], i["title"], i["state"], i["created_at"])
            for i in issues]
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO raw_issues (id, repo_name, title, state, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, rows)
    conn.commit()
    return len(rows)