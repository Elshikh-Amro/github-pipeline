import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
BASE = "https://api.github.com"


def _get(url, params=None, max_retries=3):
    for attempt in range(max_retries):
        resp = requests.get(url, headers=HEADERS, params=params)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (401, 403) and "rate limit" in resp.text.lower():
            wait = int(resp.headers.get("Retry-After", 60))
            print(f"Rate limited. Waiting {wait}s...")
            time.sleep(wait)
            continue
        if resp.status_code == 404:
            return None
        print(f"HTTP {resp.status_code} for {url}, retry {attempt+1}/{max_retries}")
        time.sleep(2 ** attempt)
    return None


def fetch_top_repos(num=50, language="python"):
    repos = []
    page = 1
    while len(repos) < num:
        data = _get(f"{BASE}/search/repositories", {
            "q": f"language:{language}",
            "sort": "stars",
            "order": "desc",
            "per_page": min(100, num),
            "page": page,
        })
        if not data or not data.get("items"):
            break
        for r in data["items"]:
            repos.append({
                "id": r["id"],
                "name": r["name"],
                "owner": r["owner"]["login"],
                "stars": r["stargazers_count"],
                "forks": r["forks_count"],
                "language": r["language"],
                "description": r["description"],
                "url": r["html_url"],
            })
            if len(repos) >= num:
                break
        page += 1
    return repos


def fetch_commits(owner, repo, max_pages=2):
    commits = []
    for page in range(1, max_pages + 1):
        data = _get(f"{BASE}/repos/{owner}/{repo}/commits", {
            "per_page": 100,
            "page": page,
        })
        if not data:
            break
        for c in data:
            author = c.get("commit", {}).get("author", {})
            commits.append({
                "sha": c["sha"],
                "repo_name": repo,
                "author_name": author.get("name", "unknown"),
                "message": c["commit"]["message"].split("\n")[0],
                "committed_at": author.get("date"),
            })
    return commits


def fetch_issues(owner, repo):
    data = _get(f"{BASE}/repos/{owner}/{repo}/issues", {
        "state": "open",
        "per_page": 50,
    })
    if not data:
        return []
    return [{
        "id": i["id"],
        "repo_name": repo,
        "title": i["title"],
        "state": i["state"],
        "created_at": i["created_at"],
    } for i in data]