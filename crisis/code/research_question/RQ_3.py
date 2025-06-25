import requests, pandas as pd, time, logging
from urllib.parse import quote_plus
from tqdm.auto import tqdm

df  = pd.read_csv("C:/Users/annan/Downloads/mashup_IRIS_subset.csv", low_memory=False)   
list_doi = df["doi"].dropna().tolist()             


BASE = "https://opencitations.net/index/api/v2"
HEADERS = {"User-Agent": "CitationsBot/0.1 (mailto:you@example.com)"}

logging.basicConfig(
    filename="citations_errors.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

session = requests.Session()
session.headers.update(HEADERS)

def fetch(endpoint, *, max_retries=3, pause=7):
    """Return JSON list or raise after exhausting retries."""
    for attempt in range(max_retries):
        try:
            r = session.get(endpoint, timeout=15)
            if r.status_code == 429:
                # rate-limit → wait a bit longer
                time.sleep(pause * 2)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            # ValueError covers JSON decode errors
            logging.info("attempt %d failed for %s: %s", attempt + 1, endpoint, e)
            time.sleep(pause)
    # All retries failed → raise to signal a fatal fail for this DOI
    raise RuntimeError(f"❌ all retries failed for {endpoint}")

def get_objects(kind, doi):
    """Wrap fetch() and return [], not raise, on total failure."""
    url = f"{BASE}/{kind}/doi:{quote_plus(doi)}"
    try:
        return fetch(url)
    except Exception as e:
        logging.error("%s – giving up: %s", doi, e)
        return None      # keep a sentinel to flag the miss in the table

rows = []
for doi in tqdm(list_doi):
    outgoing = get_objects("references", doi)
    ingoing  = get_objects("citations",  doi)

    # When fetch failed, outgoing or ingoing will be None
    out_set = {c["oci"] for c in outgoing or [] if c.get("oci")}
    in_set  = {c["oci"] for c in ingoing  or [] if c.get("oci")}

    rows.append({
        "doi": doi,
        "cit_num_outgoing": None if outgoing is None else len(out_set),
        "cit_num_ingoing" : None if ingoing  is None else len(in_set),
        "oci_outgoing"     : list(out_set) if outgoing is not None else None,
        "oci_ingoing"      : list(in_set)  if ingoing  is not None else None,
        "status"           : (
            "failed_both" if outgoing is None and ingoing is None else
            "failed_outgoing" if outgoing is None else
            "failed_ingoing"  if ingoing  is None else
            "ok"
        )
    })
    time.sleep(1)   

output = pd.DataFrame(rows)
output.to_csv("CitationsCount.csv", index=False)
