import pandas as pd, re, unicodedata, string, itertools, numpy as np
from pathlib import Path

# ------------------------------------------------------------------
# 0.  I/O paths
# ------------------------------------------------------------------
DATA_DIR = Path("Data")
INPUT  = DATA_DIR / "mashup_IRIS_subset_v3.csv"
OUTPUT = DATA_DIR / "duplicate_objects_v3.csv"

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

# ------------------------------------------------------------------
# 3.  Pair-wise overlap tallies
# ------------------------------------------------------------------
def repo_pairs(series):
    pairs = {}
    for _, repos in series.items():
        for a, b in itertools.combinations(sorted(repos), 2):
            pairs[(a, b)] = pairs.get((a, b), 0)+1
    return pairs

doi_pairs   = repo_pairs(df[df['doi'].isin(dupe_doi.index)]
                           .groupby('doi')['src_repo'].unique())
title_pairs = repo_pairs(df[df['title_norm'].isin(dupe_title.index)]
                           .groupby('title_norm')['src_repo'].unique())


# ------------------------------------------------------------------
# 4.  Build the full duplicate-rows dataframe
# ------------------------------------------------------------------
mask_doi   = df['doi'].isin(dupe_doi.index)
mask_title = df['title_norm'].isin(dupe_title.index)

duplicates_df = df[mask_doi | mask_title].copy()

# ------------------------------------------------------------------
# 5.  Tag each row with the reason it is a duplicate
# ------------------------------------------------------------------
# create masks that match the *rows inside* duplicates_df
mask_doi_sub   = duplicates_df['doi'].isin(dupe_doi.index)
mask_title_sub = duplicates_df['title_norm'].isin(dupe_title.index)

duplicates_df['dup_reason'] = np.select(
    [mask_doi_sub & mask_title_sub, mask_doi_sub, mask_title_sub],
    ['doi & title', 'doi', 'title'],
    default=''
)

# ------------------------------------------------------------------
# 6.  Write the duplicates to disk
# ------------------------------------------------------------------
duplicates_df.to_csv(OUTPUT, index=False)
print(f"Saved {len(duplicates_df):,} duplicate rows to {OUTPUT}")
