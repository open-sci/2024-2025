import os
import json
import time
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RetryError, Timeout
from urllib3.util.retry import Retry

base_dir = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(base_dir, "swh_output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# timeout handling
def safe_request(method, url, session, max_attempts=5, timeout=15, **kwargs):
    """
    Performs session.request(method, url) with:
      - honoring Retry-After on HTTP 429, but on the second consecutive 429 pauses for 1 hour before retrying
      - exponential backoff on timeouts / network errors (up to max_attempts)
      - raises after max_attempts timeouts / network errors
    """
    attempt = 0
    rate_limit_hits = 0

    while True:
        attempt += 1
        try:
            resp = session.request(method, url, timeout=timeout, **kwargs)
        except Timeout:
            if attempt >= max_attempts:
                raise
            wait = 2 ** (attempt - 1)
            print(f"[Timeout] {method} {url} (attempt {attempt}/{max_attempts}), retrying in {wait}s…")
            time.sleep(wait)
            continue
        except RetryError:
            if attempt >= max_attempts:
                raise
            wait = 2 ** (attempt - 1)
            print(f"[Network error] {method} {url} (attempt {attempt}/{max_attempts}), retrying in {wait}s…")
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            rate_limit_hits += 1
            if rate_limit_hits >= 2:
                print(f"[429] {method} {url} – second rate-limit hit, sleeping for 1 hour…")
                time.sleep(3600)
                rate_limit_hits = 0
            else:
                ra = resp.headers.get("Retry-After", "")
                wait = int(ra) if ra.isdigit() else 60
                print(f"[429] {method} {url} – retrying after {wait}s…")
                time.sleep(wait)
            continue

        rate_limit_hits = 0
        resp.raise_for_status()
        return resp

# Configuration - files in OUTPUT_DIR
KEYWORDS         = ["unibo", "unibo.it", "alma mater", "alma mater studiorum",
                    "university of bologna", "almamaterstudiorum",
                    "università di bologna", "universita di bologna"]
API_BASE         = "https://archive.softwareheritage.org/api/1"
SEARCH_PARAMS    = {"with_visit": "true", "limit": 1000}
CACHE_FILE       = os.path.join(OUTPUT_DIR, "cached_candidate_origins.json")
PROCESSED_FILE   = os.path.join(OUTPUT_DIR, "processed_origins.json")
CONFIRMED_FILE   = os.path.join(OUTPUT_DIR, "unibo_repositories_swh.json")
DEFAULT_TIMEOUT  = 15
LOG_LIMIT        = 200  # page size for log pagination

# Prepare HTTP session
session = requests.Session()
session.headers.update({
    "Accept": "application/json"
})
adapter = HTTPAdapter(max_retries=Retry(
    total=3, backoff_factor=0.5,
    status_forcelist=[500,502,503,504],
    allowed_methods=["GET","POST"]
))
session.mount("https://", adapter)
session.mount("http://", adapter)

# 1. Load or fetch candidate origins
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        origins = json.load(f)
    print(f"Loaded {len(origins)} origins from cache.")
else:
    all_entries = []
    for kw in KEYWORDS:
        print(f"Searching '{kw}'…")
        resp = safe_request("GET", f"{API_BASE}/origin/search/{kw}/",
                            session, timeout=DEFAULT_TIMEOUT, params=SEARCH_PARAMS)
        all_entries.extend(resp.json())
        while 'Link' in resp.headers:
            links = requests.utils.parse_header_links(
                resp.headers['Link'].rstrip('>').replace('>,','>,')
            )
            nxt = next((l['url'] for l in links if l.get('rel') == 'next'), None)
            if not nxt:
                break
            resp = safe_request("GET", nxt, session, timeout=DEFAULT_TIMEOUT)
            all_entries.extend(resp.json())
    unique = {e['url']: e for e in all_entries if isinstance(e, dict) and 'url' in e}
    origins = list(unique.values())
    print(f"Found {len(origins)} unique origins; caching…")
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(origins, f, indent=2)

# fetch complete revision history with automatic pagination
import requests.utils

def fetch_revision_log(rev):
    logs = []
    params = {"limit": LOG_LIMIT}
    url = f"{API_BASE}/revision/{rev}/log/"

    while url:
        resp = safe_request("GET", url, session, timeout=DEFAULT_TIMEOUT, params=params)
        data = resp.json()
        logs.extend(data)
        link_hdr = resp.headers.get("Link", "")
        next_url = None
        if link_hdr:
            links = requests.utils.parse_header_links(
                link_hdr.rstrip('>').replace('>,','>,')
            )
            next_url = next((l['url'] for l in links if l.get('rel') == 'next'), None)
        url = next_url
        params = None
    return logs

# 2. Filter and confirm UNIBO repos with resume support
last_idx = -1
if os.path.exists(PROCESSED_FILE):
    with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
        last_idx = json.load(f).get("last_index", -1)
confirmed = []
if os.path.exists(CONFIRMED_FILE):
    with open(CONFIRMED_FILE, "r", encoding="utf-8") as f:
        confirmed = json.load(f)

for idx, entry in enumerate(origins):
    if idx <= last_idx:
        continue
    url = entry.get('url')
    print(f"\n[{idx}/{len(origins)}] Checking {url}")
    last_idx = idx
    with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_index": last_idx}, f)

    if not url:
        continue

    # 2.1 Latest snapshot
    lv = safe_request("GET", f"{API_BASE}/origin/{url}/visit/latest/", session,
                      timeout=DEFAULT_TIMEOUT, params={"require_snapshot": "true"}).json()
    snap_id = lv.get("snapshot")
    if not snap_id:
        print(" → no snapshot, skip")
        continue

    # 2.2 Snapshot branches
    snap = safe_request("GET", f"{API_BASE}/snapshot/{snap_id}/", session,
                        timeout=DEFAULT_TIMEOUT).json()
    branches = snap.get("branches", {})
    if not branches:
        print(" → no branches, skip")
        continue
    if 'refs/heads/master' in branches:
        br = branches['refs/heads/master']
    elif 'refs/heads/main' in branches:
        br = branches['refs/heads/main']
    else:
        br = next(iter(branches.values()))
    if br.get("target_type") != "revision":
        print(" → branch not revision, skip")
        continue
    rev = br["target"]

    # 2.3 Revision metadata
    rev_meta = safe_request("GET", f"{API_BASE}/revision/{rev}/", session,
                            timeout=DEFAULT_TIMEOUT).json()
    dir_id = rev_meta.get("directory")

    # 2.4 Directory listing
    entries = safe_request("GET", f"{API_BASE}/revision/{rev}/directory/", session,
                           timeout=DEFAULT_TIMEOUT).json().get("content", [])

    # 2.5 README check
    readme_ok = False
    for e in entries:
        if e.get("type") == "file" and e.get("name", "").lower().startswith("readme"):
            blob = e.get("target")
            txt = safe_request("GET",
                f"{API_BASE}/content/sha1_git:{blob}/raw/", session,
                timeout=DEFAULT_TIMEOUT).text.lower()
            if any(k in txt for k in ["unibo.it", "università di bologna", "university of bologna", "alma mater studiorum"]):
                readme_ok = True
            break

    # 2.6 Bulk log for authors
    try:
        log = fetch_revision_log(rev)
    except Exception as e:
        print(f"  [Error fetching log] {e} — skip repo")
        continue
    authors = set()
    author_ok = False
    for role in ("author", "committer"):
        a = rev_meta.get(role, {}) or {}
        name = (a.get("name") or a.get("fullname") or "").strip()
        email = (a.get("email") or "").strip()
        if name or email:
            authors.add((name, email))
            if "unibo.it" in email.lower():
                author_ok = True
    for rec in log:
        for role in ("author", "committer"):
            a = rec.get(role, {}) or {}
            name = (a.get("name") or a.get("fullname") or "").strip()
            email = (a.get("email") or "").strip()
            if name or email:
                authors.add((name, email))
                if "unibo.it" in email.lower():
                    author_ok = True

    if readme_ok or author_ok:
        auth_list = [{"name": n, "email": e} for (n, e) in sorted(authors)]
        confirmed.append({"url": url, "rev": rev, "dir_id": dir_id, "authors": auth_list})
        print(f" → confirmed UNIBO repo; authors: {auth_list}")
        with open(CONFIRMED_FILE, "w", encoding="utf-8") as f:
            json.dump(confirmed, f, indent=2)
    else:
        print(" → not UNIBO, skip")

print(f"\nFiltering done: {len(confirmed)} confirmed repos.")