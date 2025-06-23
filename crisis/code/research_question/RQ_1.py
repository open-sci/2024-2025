# RQ1.py  –  Coverage of UniBo “non-traditional” research objects in repositories
# ------------------------------------------------------------------------------

import re
from pathlib import Path
import pandas as pd

# ----------------------------------------------------------------------
# 0. CONFIGURATION
# ----------------------------------------------------------------------
DATA_FILE = Path("Data/mashup_complete.csv")
OUT_CSV   = Path("Data/coverage_by_iris_and_repository1.csv")

# ----------------------------------------------------------------------
# 1. LOAD DATA
# ----------------------------------------------------------------------
df = pd.read_csv(DATA_FILE, low_memory=False)

# Normalise the resource_type for matching  ----------------------------
df["resource_type"] = df["resource_type"].str.strip()

# ---------- Fix the missing label problem -----------------------
is_swh = df["src_repo"].str.strip().str.lower() == "software heritage"
df.loc[is_swh & df["resource_type"].isna(), "resource_type"] = "Software"


required = {"resource_type", "src_repo", "description"}
missing  = required.difference(df.columns)
if missing:
    raise KeyError(f"Missing column(s) in CSV: {', '.join(missing)}")

# ----------------------------------------------------------------------
# 2. LOOK-UP TABLE  (everything not handled by heuristics)
# ----------------------------------------------------------------------
iris_map = {
    # 7.04 – Software
    "Software": "7.04 Software",
    "Computational notebook": "7.04 Software",

    # 7.05 – Databases / structured data
    "Dataset": "7.05 Banche dati",
    "Annotation collection": "7.05 Banche dati",
    "Taxonomic treatment": "7.05 Banche dati",
    "Data paper": "7.05 Banche dati",

    # 7.13 – Technical reports & similar documents
    "Report": "7.13 Rapporto tecnico",
    "Technical note": "7.13 Rapporto tecnico",
    "technical_report": "7.13 Rapporto tecnico",
    "Project deliverable": "7.13 Rapporto tecnico",
    "Project milestone": "7.13 Rapporto tecnico",
    "Output management plan": "7.13 Rapporto tecnico",
    "Proposal": "7.13 Rapporto tecnico",
    "Standard": "7.13 Rapporto tecnico",
    "manual": "7.13 Rapporto tecnico",
    "Software documentation": "7.13 Rapporto tecnico",

    # 7.14 – Audiovisuals & learning objects
    "Video/Audio": "7.14 Audiovisivi",
    "Presentation": "7.14 Audiovisivi",
    "Lesson": "7.14 Audiovisivi",

    # 7.12 – Stand-alone visual artefacts
    "Poster": "7.12 Mostra o Esposizione",
    "Image":  "7.12 Mostra o Esposizione",
    "Photo":  "7.12 Mostra o Esposizione",

    # 7.09 – Public events / performances
    "Event": "7.09 Performance",
}

# ----------------------------------------------------------------------
# 3. HEURISTIC HELPERS  -------------------------------------------------
#    (ordered most-specific → least-specific)
# ----------------------------------------------------------------------

# 3.1  Model  → choose among 7.03 / 7.04 / 7.05 / 7.10
bim_kw  = re.compile(r"\b(bim|digital twin|ricostruzion|reconstruction|3d|architecture|architectural|archaeological|colonna|temple)\b", re.I)
ml_kw   = re.compile(r"\b(weights?|trained|model files?|pytorch|keras|tensorflow|bert|rnn|schnet|ensemble|\.pt\b|\.pth\b|\.h5\b)\b", re.I)
data_kw = re.compile(r"\b(netcdf|data model|ontology|knowledge graph|schema)\b", re.I)

def model_to_iris(row):
    if row["resource_type"] != "Model":
        return None
    desc = str(row.get("description", ""))
    if re.search(bim_kw, desc):
        return "7.03 Prodotto dell’ingegneria civile e dell’architettura"
    if re.search(ml_kw, desc):
        return "7.04 Software"
    if re.search(data_kw, desc):
        return "7.05 Banche dati"
    return "7.10 Manufatto"

# 3.2  Plot  → map vs non-map
non_map_plot = re.compile(r"\b(supplementary|appendix|table[s]?|figure[s]?|plot[s]? from (the )?article)\b", re.I)

def plot_to_iris(row):
    if row["resource_type"] != "Plot":
        return None
    desc = str(row["description"] or "")
    return ("7.12 Mostra o Esposizione" if re.search(non_map_plot, desc)
            else "7.01 Carta tematica e geografica")

# 3.3  Diagram  → map vs generic
map_clue = re.compile(r"\b(distribution map|map of|thematic map|geographical map|cartography|grid[- ]based map)\b", re.I)

def diagram_to_iris(row):
    if row["resource_type"] != "Diagram":
        return None
    desc = str(row["description"] or "")
    return ("7.01 Carta tematica e geografica" if re.search(map_clue, desc)
            else "7.12 Mostra o Esposizione")

# 3.4  Figure  → map vs generic
map_hint = re.compile(r"\b(distribution map|grid[- ]based map|geological map|map of|maps? showing)\b", re.I)

def figure_to_iris(row):
    if row["resource_type"] != "Figure":
        return None
    desc = str(row["description"] or "")
    return ("7.01 Carta tematica e geografica" if re.search(map_hint, desc)
            else "7.12 Mostra o Esposizione")

# 3.5  Workflow  → executable vs document
code_sig = re.compile(r"\b(Galaxy|Nextflow|Snakemake|CWL|WDL|\.ga\b|\.cwl\b|\.nf\b|\.smk\b)\b", re.I)

def workflow_to_iris(row):
    if row["resource_type"] != "Workflow":
        return None
    desc = str(row["description"] or "")
    return ("7.04 Software" if re.search(code_sig, desc)
            else "7.13 Rapporto tecnico")

# ----------------------------------------------------------------------
# 4. BUILD  iris_type  (helpers first → lookup → Unclassified)
# ----------------------------------------------------------------------
df["iris_type"] = (
      df.apply(model_to_iris,    axis=1)
        .fillna(df.apply(plot_to_iris,     axis=1))
        .fillna(df.apply(diagram_to_iris,  axis=1))
        .fillna(df.apply(figure_to_iris,   axis=1))
        .fillna(df.apply(workflow_to_iris, axis=1))
        .fillna(df["resource_type"].map(iris_map))
        .fillna("Unclassified")
)

# ----------------------------------------------------------------------
# 5. COUNT  (repository × IRIS 7.*)   –  exclude out-of-scope rows
# ----------------------------------------------------------------------
mask_7 = df["iris_type"].str.startswith("7.")
coverage_long = (
    df[mask_7]
      .groupby(["src_repo", "iris_type"], dropna=False)
      .size()
      .reset_index(name="n")
)

coverage = (
    coverage_long
      .pivot(index="iris_type", columns="src_repo", values="n")
      .fillna(0)
      .astype(int)
      .sort_index()
)

# ----------------------------------------------------------------------
# 6. SAVE  &  REPORT
# ----------------------------------------------------------------------
coverage.to_csv(OUT_CSV, encoding="utf-8")
print("\n=== RQ 1 – Coverage table (IRIS 7.* only) ===")
print(coverage)

# get every record whose iris_type falls in the IRIS 7.* subtree
records_7 = df[df["iris_type"].str.startswith("7.")].copy()
records_7.to_csv("Data/all_records_7star.csv", index=False, encoding="utf-8")
