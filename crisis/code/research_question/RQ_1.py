# RQ1.py  –  Coverage of UniBo “non-traditional” research objects in repositories
# ------------------------------------------------------------------------------

import pandas as pd
import re
from pathlib import Path

# ------------------------------------------------------------
# 1.  File location
# ------------------------------------------------------------
DATA_DIR = Path("Data")
PATH = DATA_DIR / "mashup_v3.csv"

# ------------------------------------------------------------
# 2.  IRIS mapping – a few extra synonyms seen in v3
# ------------------------------------------------------------
iris_map = {
    "7.01 Carta tematica e geografica": [
        r"7\.01", r"carta\s+(tematica|geografica)"
    ],
    "7.02 Carta geologica": [r"7\.02", r"carta\s+geologica"],
    "7.03 Prodotto dell’ingegneria civile e dell’architettura": [
        r"7\.03", r"prodotto.*ingegneria", r"model", r"physical\s*object"
    ],
    "7.04 Software": [
        r"7\.04", r"\bsoftware\b", r"workflow",
        r"computational notebook", r"software\s*documentation"
    ],
    "7.05 Banche dati": [
        r"7\.05", r"banche\s+dati", r"dataset",
        r"annotation collection", r"database"
    ],
    "7.06 Composizione musicale": [
        r"7\.06", r"composizione\s+musicale", r"audio"
    ],
    "7.07 Disegno": [
        r"7\.07", r"disegno", r"(figure|image|diagram)"
    ],
    "7.08 Design": [r"7\.08", r"\bdesign\b"],
    "7.09 Performance": [r"7\.09", r"\bperformance\b", r"event"],
    "7.10 Manufatto": [
        r"7\.10", r"manufatto", r"(physical object|model|prototype)"
    ],
    "7.11 Prototipo d'arte e relativi progetti": [r"7\.11", r"prototipo"],
    "7.12 Mostra o Esposizione": [
        r"7\.12", r"(mostra|esposizione|exhibition|event)"
    ],
    "7.13 Rapporto tecnico": [
        r"7\.13", r"rapporto\s+tecnico",
        r"(technical note|project deliverable|report)"
    ],
    "7.14 Audiovisivi": [
        r"7\.14", r"(video|audiovisivi|video/audio|poster\+video)"
    ],
    "7.15 Test psicologici": [
        r"7\.15", r"(test psicologici|psychological test|psychometrics)"
    ],
}

# ------------------------------------------------------------
# 3.  Row-level classifier
# ------------------------------------------------------------
def classify(row):
    txt = f"{row['type']} {row['resource_type']}".lower()
    for iris, patterns in iris_map.items():
        if any(re.search(pat, txt) for pat in patterns):
            return iris
    return pd.NA

# ------------------------------------------------------------
# 4.  Streaming read, labelling, and export
# ------------------------------------------------------------
chunksize = 100_000
filtered_chunks = []

for chunk in pd.read_csv(PATH, chunksize=chunksize, low_memory=False):
    # unify case ONCE so subsequent filters are case-safe
    chunk['src_repo'] = chunk['src_repo'].str.lower()
    chunk['type']     = chunk['type'].str.lower()

    chunk["iris_cat"] = chunk.apply(classify, axis=1)
    filtered_chunks.append(chunk.dropna(subset=["iris_cat"]))

iris_df = pd.concat(filtered_chunks, ignore_index=True)

# save the rich subset (all original columns + iris_cat)
iris_df.to_csv(DATA_DIR / "mashup_IRIS_subset_v3.csv", index=False)

# ------------------------------------------------------------
# 5.  RQ 1 coverage table
# ------------------------------------------------------------
coverage = (iris_df
            .groupby(['src_repo', 'iris_cat'])
            .size()
            .unstack(fill_value=0)
            .sort_index())

print(coverage)

