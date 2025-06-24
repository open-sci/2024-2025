# RQ1.py  –  Coverage of UniBo “non-traditional” research objects in repositories
# ------------------------------------------------------------------------------

import pandas as pd
import re

# ------------ adjust if the CSV lives elsewhere -------------
PATH = "Data/mashup_v2.csv"

# ------------ the mapping dictionary ------------------------
iris_map = {
    "7.01 Carta tematica e geografica": [
        r"7\.01", r"carta\s+(tematica|geografica)"
    ],
    "7.02 Carta geologica": [r"7\.02", r"carta\s+geologica"],
    "7.03 Prodotto dell’ingegneria civile e dell’architettura": [
        r"7\.03", r"prodotto.*ingegneria", r"model", r"physical\s*object"
    ],
    "7.04 Software": [r"7\.04", r"\bsoftware\b", r"workflow",
                      r"computational notebook"],
    "7.05 Banche dati": [r"7\.05", r"banche\s+dati", r"dataset",
                         r"annotation collection", r"database"],
    "7.06 Composizione musicale": [r"7\.06", r"composizione\s+musicale",
                                   r"audio"],
    "7.07 Disegno": [r"7\.07", r"disegno", r"(figure|image|diagram)"],
    "7.08 Design": [r"7\.08", r"\bdesign\b"],
    "7.09 Performance": [r"7\.09", r"\bperformance\b", r"event"],
    "7.10 Manufatto": [r"7\.10", r"manufatto", r"(physical object|model|prototype)"],
    "7.11 Prototipo d'arte e relativi progetti": [r"7\.11", r"prototipo"],
    "7.12 Mostra o Esposizione": [r"7\.12", r"(mostra|esposizione|exhibition|event)"],
    "7.13 Rapporto tecnico": [r"7\.13", r"rapporto\s+tecnico", r"(technical note|project deliverable|report)"],
    "7.14 Audiovisivi": [r"7\.14", r"(video|audiovisivi|video/audio)"],
    "7.15 Test psicologici": [r"7\.15", r"(test psicologici|psychological test|psychometrics)"],
}

# ------------ helper: classify one row ----------------------
def classify(row):
    txt = f"{row['type']} {row['resource_type']}".lower()
    for iris, patterns in iris_map.items():
        if any(re.search(pat, txt) for pat in patterns):
            return iris
    return pd.NA

# ------------ chunk processing ------------------
usecols = None
chunksize = 100_000
filtered_chunks = []

for chunk in pd.read_csv(PATH, usecols=usecols,
                         chunksize=chunksize, low_memory=False):
    chunk["iris_cat"] = chunk.apply(classify, axis=1)
    filtered_chunks.append(chunk.dropna(subset=["iris_cat"]))

iris_df = pd.concat(filtered_chunks, ignore_index=True)
# ------------------------------------------------------------

iris_df.to_csv("mashup_IRIS_subset.csv", index=False)   # master subset for RQ 1

# First-level counts for RQ 1
coverage = (iris_df.groupby(["src_repo", "iris_cat"])
                      .size()
                      .unstack(fill_value=0)
                      .sort_index())
print(coverage)
