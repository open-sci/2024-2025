import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots



df = pd.read_csv(r'data\mashup_IRIS_subset_v3.csv')

# Normalize date strings and accept raw year-only values like "2006" or "2013.0"
df["date_str"] = df["date"].astype(str).str.strip()
df["date_str"] = df["date_str"].str.replace(r"\.0+$", "", regex=True)

# Use strict year parse first (faster, determinist) and fallback to general parse
df["date"] = pd.to_datetime(df["date_str"], format="%Y", errors="coerce")
df["date"] = df["date"].fillna(pd.to_datetime(df["date_str"], errors="coerce"))

df = df[df["date"].notna()]

iris = df[df["src_repo"].str.strip().str.lower() == "iris"]
'''print("iris after date filter:", len(iris))
print("any date NaT left:", df["date"].isna().sum())
print("year range:", df["date"].dt.year.min(), df["date"].dt.year.max())
print("iris rows after date filter:", len(iris))'''

# Sort for consistent plotting
df = df.sort_values(by="date")

years = sorted(set(int(y) for y in df["date"].dt.year.unique() if not pd.isna(y)))
years = [y for y in years if y >= 2004]

if not years:
    raise SystemExit("No valid years found in the dataset")

# IRIS categories (ordered list used for stacking)
iris_categories = [
    "7.01 Carta tematica e geografica",
    "7.02 Carta geologica",
    "7.03 Prodotto dell’ingegneria civile e dell’architettura",
    "7.04 Software",
    "7.05 Banche dati",
    "7.06 Composizione musicale",
    "7.07 Disegno",
    "7.08 Design",
    "7.09 Performance",
    "7.10 Manufatto",
    "7.11 Prototipo d'arte e relativi progetti",
    "7.12 Mostra o Esposizione",
    "7.13 Rapporto tecnico",
    "7.14 Audiovisivi",
    "7.15 Test psicologici",
]

object_types = {
    "7.01": ["carta tematica", "carta geografica"],
    "7.02": ["carta geologica"],
    "7.03": ["prodotto dell’ingegneria civile e dell’architettura", "model", "physical object"],
    "7.04": ["software", "workflow", "computational notebook", "software documentation"],
    "7.05": ["banche dati", "dataset", "annotation collection", "database"],
    "7.06": ["composizione musicale", "audio"],
    "7.07": ["disegno", "figure", "image", "diagram"],
    "7.08": ["design"],
    "7.09": ["performance", "event"],
    "7.10": ["manufatto", "physical object", "model", "prototype"],
    "7.11": ["prototipo"],
    "7.12": ["mostra", "esposizione", "exhibition", "event"],
    "7.13": ["rapporto tecnico", "technical note", "project deliverable", "report"],
    "7.14": ["video", "audiovisivi", "video/audio", "poster+video"],
    "7.15": ["test psicologici", "psychological test", "psychometrics"]
}

def lookup_iris_category(raw):
    if pd.isna(raw):
        return None
    v = str(raw).strip().lower()

    # exact match on iris_categories names
    for cat in iris_categories:
        if v == cat.lower():
            return cat

    # fallback by object_types list phrases (contain check, case-insensitive)
    for cat_key, patterns in object_types.items():
        for p in patterns:
            if p in v:
                # find full label by key prefix
                return next((c for c in iris_categories if c.startswith(cat_key)), None)

    return None

df["iris_cat_mapped"] = df["iris_cat"].apply(lookup_iris_category)
df = df[df["iris_cat_mapped"].notna()].copy()

# Sources (data key, display title)
sources = [
    ("zenodo", "Zenodo"),
    ("amsacta", "AMS Acta"),
    ("iris", "IRIS"),
    #("software heritage", "Software Heritage"),
]

fig = make_subplots(
    rows=3,
    cols=1,
    subplot_titles=[display for _, display in sources],
    shared_xaxes=True,
    vertical_spacing=0.15,
)
fig.update_layout(
    barmode="stack",
    title_text="Timeline for non traditional research outputs",
    #xaxis_title="Year",
    yaxis_title="Count",
)

for r in range(1, 4):
    fig.update_xaxes(
        row=r, col=1,
        tickmode="linear",
        tick0=2004,
        dtick=1,
        showticklabels=True,
        tickangle=45,
    )
    fig.update_yaxes(
        row=r, col=1,
        range=[0, 650],
        autorange=False
    )

# Each subplot shows all years (one bar per year) stacked by the IRIS category.
for idx, (src_key, _) in enumerate(sources):
    row = idx + 1
    col = 1

    df_src = df[df["src_repo"].str.strip().str.lower() == src_key]

    for cat in iris_categories:
        counts = []
        for y in years:
            counts.append(int(((df_src["date"].dt.year == y) &
                               (df_src["iris_cat_mapped"] == cat)).sum()))

        fig.add_trace(
            go.Bar(
                x=years,
                y=counts,
                name=cat,
                showlegend=(idx == 0),
                marker_line_width=0,
            ),
            row=row,
            col=col,
        )

"""fig.show(renderer="browser")"""
pio.write_image(fig, "timegraph.jpeg", width=1400, height=900, scale=2)