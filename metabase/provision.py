import os
import time

import requests

METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000")
ADMIN_EMAIL = os.getenv("METABASE_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.getenv("METABASE_PASSWORD", "metabase-pass-123")
SITE_NAME = os.getenv("METABASE_SITE_NAME", "GitHub Analytics")

# Postgres source that Metabase connects to (resolves on the docker network)
PG_HOST = os.getenv("MB_PG_HOST", "postgres")
PG_PORT = os.getenv("MB_PG_PORT", "5432")
PG_DB = os.getenv("MB_PG_DB", "github_analytics")
PG_USER = os.getenv("MB_PG_USER", "postgres")
PG_PASSWORD = os.getenv("MB_PG_PASSWORD", "postgres")

TIMEOUT = 30


def wait_for_health(attempts=60, delay=2):
    print("Waiting for Metabase to be healthy...")
    for i in range(attempts):
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if r.status_code == 200 and r.json().get("status") == "ok":
                print("Metabase is up.")
                return
        except requests.RequestException:
            pass
        time.sleep(delay)
    raise SystemExit("Metabase did not become healthy in time.")


def get_session():
    r = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
        timeout=TIMEOUT,
    )
    if r.status_code == 200:
        return r.json()["id"]
    return None


def get_setup_token():
    r = requests.get(f"{METABASE_URL}/api/session/properties", timeout=TIMEOUT)
    r.raise_for_status()
    return r.json().get("setup-token")


def setup_new_instance():
    print("No existing session found — running first-time setup...")
    token = get_setup_token()
    if not token:
        raise SystemExit("Metabase reports no setup token (already initialized?).")
    payload = {
        "token": token,
        "prefs": {"site_name": SITE_NAME, "allow_tracking": False},
        "user": {
            "first_name": "Admin",
            "last_name": "GitHub",
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
            "site_name": SITE_NAME,
        },
        "database": {
            "engine": "postgres",
            "name": "GitHub Analytics",
            "details": {
                "host": PG_HOST,
                "port": int(PG_PORT),
                "dbname": PG_DB,
                "user": PG_USER,
                "password": PG_PASSWORD,
                "ssl": False,
                "tunnel-enabled": False,
            },
        },
    }
    r = requests.post(f"{METABASE_URL}/api/setup", json=payload, timeout=TIMEOUT)
    if r.status_code not in (200, 201):
        raise SystemExit(f"Setup failed ({r.status_code}): {r.text}")
    print("Metabase setup complete.")
    return r.json().get("id")


def api(session_id, method, path, **kwargs):
    headers = {"X-Metabase-Session": session_id}
    headers["Content-Type"] = "application/json" if "json" in kwargs else None
    r = requests.request(method, f"{METABASE_URL}{path}", headers=headers, timeout=TIMEOUT, **kwargs)
    if r.status_code >= 400:
        raise SystemExit(f"API {method} {path} failed ({r.status_code}): {r.text}")
    return r.json()


def get_database_id(session_id):
    dbs = api(session_id, "GET", "/api/database")
    dbs = dbs if isinstance(dbs, list) else dbs.get("data", [])
    for db in dbs:
        if db.get("name") == "GitHub Analytics":
            return db["id"]
    return None


def ensure_database(session_id):
    db_id = get_database_id(session_id)
    if db_id:
        print(f"Database 'GitHub Analytics' already exists (id={db_id}).")
        return db_id
    payload = {
        "engine": "postgres",
        "name": "GitHub Analytics",
        "details": {
            "host": PG_HOST,
            "port": int(PG_PORT),
            "dbname": PG_DB,
            "user": PG_USER,
            "password": PG_PASSWORD,
            "ssl": False,
            "tunnel-enabled": False,
        },
    }
    created = api(session_id, "POST", "/api/database", json=payload)
    print(f"Created database 'GitHub Analytics' (id={created['id']}).")
    return created["id"]


CARDS = [
    {
        "name": "Trending Repos",
        "display": "bar",
        "description": "Top Python repos by stars",
        "sql": """
            SELECT repo_name, stars
            FROM public_marts.dim_repositories
            ORDER BY stars DESC
            LIMIT 15
        """,
    },
    {
        "name": "Commit Velocity",
        "display": "line",
        "description": "Commits per day across tracked repos",
        "sql": """
            SELECT date_day, COUNT(*) AS num_commits
            FROM public_marts.fact_commits
            GROUP BY date_day
            ORDER BY date_day
        """,
    },
    {
        "name": "Issue Closure by Repo",
        "display": "bar",
        "description": "Open vs closed issues per repository",
        "sql": """
            SELECT repo_name,
                   COUNT(*) FILTER (WHERE is_closed) AS closed_issues,
                   COUNT(*) FILTER (WHERE NOT is_closed) AS open_issues
            FROM public_marts.fact_issues
            GROUP BY repo_name
            ORDER BY closed_issues DESC
        """,
    },
]


def dashboard_cards_to_dashcards(session_id, dash_id, card_ids):
    """Return dashcards payload (with negative ids for new cards) for the given card ids."""
    full = api(session_id, "GET", f"/api/dashboard/{dash_id}")
    existing = {dc.get("card_id") for dc in full.get("dashcards", [])}
    new_cards = [cid for cid in card_ids if cid not in existing]
    payload = []
    for i, cid in enumerate(new_cards):
        payload.append({
            "id": -1 - i,
            "card_id": cid,
            "col": 0,
            "row": i * 4,
            "size_x": 12,
            "size_y": 4,
            "series": [],
            "parameter_mappings": [],
            "visualization_settings": {},
        })
    return payload, len(new_cards)


def provision(session_id):
    db_id = ensure_database(session_id)
    existing_cards = api(session_id, "GET", "/api/card")
    existing_cards = existing_cards if isinstance(existing_cards, list) else existing_cards.get("data", [])
    for card in CARDS:
        found = next((c for c in existing_cards if c.get("name") == card["name"]), None)
        if found:
            print(f"Card '{card['name']}' already exists (id={found['id']}) — skipping.")
            card["id"] = found["id"]
            continue

        payload = {
            "name": card["name"],
            "display": card["display"],
            "description": card["description"],
            "dataset_query": {
                "database": db_id,
                "type": "native",
                "native": {"query": card["sql"], "template-tags": {}},
            },
            "visualization_settings": {},
            "collection_position": None,
        }
        created = api(session_id, "POST", "/api/card", json=payload)
        card["id"] = created["id"]
        print(f"Created card '{card['name']}' (id={created['id']}).")

    dashboards = api(session_id, "GET", "/api/dashboard")
    dashboards = dashboards if isinstance(dashboards, list) else dashboards.get("data", [])
    dash = next((d for d in dashboards if d.get("name") == "GitHub Analytics"), None)
    if dash is None:
        dash = api(
            session_id,
            "POST",
            "/api/dashboard",
            json={"name": "GitHub Analytics", "description": "Trending repos, commit velocity, issue closure"},
        )
        print(f"Created dashboard (id={dash['id']}).")
    dash_id = dash["id"]

    card_ids = [c["id"] for c in CARDS if c.get("id")]
    dashcards, added = dashboard_cards_to_dashcards(session_id, dash_id, card_ids)
    if added:
        api(
            session_id,
            "PUT",
            f"/api/dashboard/{dash_id}",
            json={"name": dash["name"], "description": dash.get("description"), "dashcards": dashcards},
        )
        print(f"Added {added} cards to dashboard.")
    else:
        print("Dashboard already contains all cards.")

    print(f"\nDone! Open your dashboard: {METABASE_URL}/dashboard/{dash_id}")


def find_dashboard_url(session_id):
    dashboards = api(session_id, "GET", "/api/dashboard")
    dashboards = dashboards if isinstance(dashboards, list) else dashboards.get("data", [])
    dash = next((d for d in dashboards if d.get("name") == "GitHub Analytics"), None)
    if dash:
        return f"{METABASE_URL}/dashboard/{dash['id']}"
    return None


def main():
    import sys

    url_only = "--print-dashboard-url" in sys.argv
    wait_for_health()
    session_id = get_session()
    if session_id is None:
        session_id = setup_new_instance()
    if not session_id:
        raise SystemExit("Could not obtain a Metabase session.")
    if url_only:
        url = find_dashboard_url(session_id)
        if not url:
            raise SystemExit("Dashboard not found.")
        print(url)
        return
    provision(session_id)


if __name__ == "__main__":
    main()