import pandas as pd, re, unicodedata, string, itertools, numpy as np
from collections import Counter
from pathlib import Path

# ------------------------------------------------------------------
# 0.  I/O paths
# ------------------------------------------------------------------
DATA_DIR = Path("Data")
INPUT  = DATA_DIR / "mashup_IRIS_subset_v3.csv"
OUTPUT = DATA_DIR / "duplicate_objects_v3.csv"
OUT_PAIR_TYPE = DATA_DIR / "repo_pair_bytype_v3.csv"
DUP_FILE = DATA_DIR / "duplicate_objects_v3.csv"
OUT_FILE = DATA_DIR / "repo_pair_bytype_v3.csv"

# ------------------------------------------------------------------
# 1.  Load & normalise minimal columns we need
# ------------------------------------------------------------------
df = pd.read_csv(INPUT)
df['src_repo'] = df['src_repo'].str.lower()

# ------------------------------------------------------------------
# 2.  Duplicate detection
# ------------------------------------------------------------------
# 2-a  DOI overlap --------------------------------------------------
dupe_doi = (df.dropna(subset=['doi'])
              .groupby('doi')['src_repo'].nunique()
              .pipe(lambda g: g[g>1]))

# 2-b  Title-normalisation helper ----------------------------------
def norm(t):
    if pd.isna(t): return None
    t = unicodedata.normalize("NFKD", t).lower()
    t = t.translate(str.maketrans('', '', string.punctuation))
    return re.sub(r'\s+', ' ', t).strip()

df['title_norm'] = df['title'].map(norm)
dupe_title = (df.dropna(subset=['title_norm'])
                .groupby('title_norm')['src_repo'].nunique()
                .pipe(lambda g: g[g>1]))

# 2-c  SWHID / PMID overlap ---------------------------------------
dupe_swh = (df.dropna(subset=['swh_id'])
              .groupby('swh_id')['src_repo']
              .nunique()
              .pipe(lambda g: g[g > 1]))

dupe_pmid = (df.dropna(subset=['pmid'])
               .groupby('pmid')['src_repo']
               .nunique()
               .pipe(lambda g: g[g > 1]))

# ------------------------------------------------------------------
# 3.  Pair-wise overlap tallies
# ------------------------------------------------------------------
def repo_pairs(series):
    pairs = {}
    for _, repos in series.items():
        for a, b in itertools.combinations(sorted(repos), 2):
            pairs[(a, b)] = pairs.get((a, b), 0)+1
    return pairs

doi_pairs   = repo_pairs(df[df['doi'].isin(dupe_doi.index)].groupby('doi')['src_repo'].unique())
title_pairs = repo_pairs(df[df['title_norm'].isin(dupe_title.index)].groupby('title_norm')['src_repo'].unique())
swh_pairs = repo_pairs(df[df['swh_id'].isin(dupe_swh.index)].groupby('swh_id')['src_repo'].unique())
pmid_pairs = repo_pairs(df[df['pmid'].isin(dupe_pmid.index)].groupby('pmid')['src_repo'].unique())


# ------------------------------------------------------------------
# 4.  Build the full duplicate-rows dataframe
# ------------------------------------------------------------------
mask_doi   = df['doi'].isin(dupe_doi.index)
mask_title = df['title_norm'].isin(dupe_title.index)
mask_swh   = df['swh_id'].isin(dupe_swh.index)
mask_pmid  = df['pmid'].isin(dupe_pmid.index)

duplicates_df = df[mask_doi | mask_title | mask_swh | mask_pmid].copy()

# ------------------------------------------------------------------
# 5.  Tag each row with the reason it is a duplicate
# ------------------------------------------------------------------
# create masks that match the *rows inside* duplicates_df
mask_doi_sub   = duplicates_df['doi'].isin(dupe_doi.index)
mask_title_sub = duplicates_df['title_norm'].isin(dupe_title.index)
mask_swh_sub   = duplicates_df['swh_id'].isin(dupe_swh.index)
mask_pmid_sub  = duplicates_df['pmid'].isin(dupe_pmid.index)


duplicates_df['dup_reason'] = np.select(
    [
        (mask_doi_sub & mask_title_sub) |
        (mask_doi_sub & mask_swh_sub)   |
        (mask_doi_sub & mask_pmid_sub)  |
        (mask_title_sub & mask_swh_sub) |
        (mask_title_sub & mask_pmid_sub)|
        (mask_swh_sub & mask_pmid_sub)  ,   # any combination of two
        mask_doi_sub,
        mask_title_sub,
        mask_swh_sub,
        mask_pmid_sub
    ],
    ['multiple ids', 'doi', 'title', 'swh_id', 'pmid'],
    default=''
)

# ------------------------------------------------------------------
# 6.  Write the duplicates to disk
# ------------------------------------------------------------------
duplicates_df.to_csv(OUTPUT, index=False)
print(f"Saved {len(duplicates_df):,} duplicate rows to {OUTPUT}")


# ------------------------------------------------------------------
# 7   Load the 285-row duplicate file
# ------------------------------------------------------------------
dupes = pd.read_csv(DUP_FILE)
dupes["src_repo"] = dupes["src_repo"].str.lower()

# ------------------------------------------------------------------
# 8   Bucket every row into software / datasets / technical reports
# ------------------------------------------------------------------
def row_bucket(row):
    t = (row.get("type") or "").lower()
    
    # -- software ---------------------------------------------------
    if ("soft" in t or "workflow" in t or "notebook" in t
        or "softwaredocumentation" in t
        or str(row.get("iris_cat","")).startswith("7.04")):
        return "software"
    
    # -- datasets ---------------------------------------------------
    if (t.startswith("data") or "dataset" in t or "database" in t
        or str(row.get("iris_cat","")).startswith("7.05")):
        return "datasets"
    
    # -- everything else → technical reports ------------------------
    return "technical reports"

dupes["row_bucket"] = dupes.apply(row_bucket, axis=1)

# ------------------------------------------------------------------
# 9   Collapse rows that are the SAME object (doi ▸ title_norm ▸ etc.)
# ------------------------------------------------------------------
def dup_key(r):
    return (r["doi"]          if pd.notna(r.get("doi"))          else
            r["title_norm"]   if pd.notna(r.get("title_norm"))   else
            r["swh_id"]       if pd.notna(r.get("swh_id"))       else
            r["pmid"])

dupes["dup_key"] = dupes.apply(dup_key, axis=1)

# ------------------------------------------------------------------
# 10   Decide ONE bucket per object
# ------------------------------------------------------------------
priority = ["software", "datasets", "technical reports"]

def object_bucket(group):
    present = set(group["row_bucket"])
    for b in priority:
        if b in present:
            return b

obj2bucket = dupes.groupby("dup_key").apply(object_bucket)

# ------------------------------------------------------------------
# 11   Count objects per repo-pair & bucket
# ------------------------------------------------------------------
pair_counter = Counter()

for obj_id, bucket in obj2bucket.items():
    rows  = dupes[dupes["dup_key"] == obj_id]
    repos = sorted(rows["src_repo"].unique())
    if len(repos) < 2:
        continue                              # not an overlap
    for a, b in itertools.combinations(repos, 2):
        pair_counter[(a, b, bucket)] += 1

# ------------------------------------------------------------------
# 12   Build the summary table and save
# ------------------------------------------------------------------
pair_df = (pd.Series(pair_counter, name="objects_shared")
             .reset_index()
             .rename(columns={"level_0":"repo_a",
                              "level_1":"repo_b",
                              "level_2":"main_duplicate_types"})
             .sort_values(["repo_a","repo_b","main_duplicate_types"]))

pair_df.to_csv(OUT_FILE, index=False)
print(pair_df)
