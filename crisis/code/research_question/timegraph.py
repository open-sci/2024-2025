import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio


df = pd.read_csv(r'data/mashup_IRIS_subset_v3.csv')
swh = pd.read_csv(r'data/SWH-output.csv')


def parse_dates(series: pd.Series) -> pd.Series:
    #parse dates that bare years (2006, 2013.0) or full dates. Always returns a Series with dtype datetime64[ns].

    cleaned = series.astype(str).str.strip().str.replace(r"\.0+$", "", regex=True)

    # Try strict year-only format first (fast path)
    parsed = pd.to_datetime(cleaned, format="%Y", errors="coerce")

    # Fill failures with a general parser
    mask = parsed.isna()
    if mask.any():
        parsed = parsed.copy()
        parsed[mask] = pd.to_datetime(cleaned[mask], errors="coerce")

    # Guarantee datetime dtype even if every value failed (avoids .dt accessor error)
    if not pd.api.types.is_datetime64_any_dtype(parsed):
        parsed = pd.to_datetime(parsed, errors="coerce")

    return parsed

df["date"] = parse_dates(df["date"])

# swh has ISO-8601 timestamps with mixed UTC offsets (+01:00 / +02:00).
# utc=True collapses them to a single datetime64[ns, UTC], then we strip
# the timezone so both dataframes are tz-naive before concat.
swh["date"] = (
    pd.to_datetime(swh["committer_date"], utc=True, errors="coerce")
    .dt.tz_localize(None)
)

# Guard: if df somehow ended up tz-aware, strip it too
if hasattr(df["date"].dtype, "tz") and df["date"].dtype.tz is not None:
    df["date"] = df["date"].dt.tz_localize(None)


df["src_repo"] = df["src_repo"].astype(str).str.strip().str.lower()
swh["src_repo"] = "software heritage"
swh["iris_cat_mapped"] = "7.04 Software"


iris_categories = [
    "7.01 Carta tematica e geografica",
    "7.02 Carta geologica",
    "7.03 Prodotto dell'ingegneria civile e dell'architettura",
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
    "7.03": ["prodotto dell'ingegneria civile e dell'architettura", "model", "physical object"],
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
    "7.15": ["test psicologici", "psychological test", "psychometrics"],
}

# Pre-build a flat lookup: phrase → full category label  (order matters: more
# specific phrases should win, so we iterate iris_categories in order)
_phrase_to_cat: dict[str, str] = {}
for cat in iris_categories:
    key = cat[:4]  # e.g. "7.04"
    for phrase in object_types.get(key, []):
        _phrase_to_cat.setdefault(phrase.lower(), cat)

def lookup_iris_category(raw) -> str | None:
    if pd.isna(raw):
        return None
    v = str(raw).strip().lower()
    # 1) exact match on full category name
    for cat in iris_categories:
        if v == cat.lower():
            return cat
    # 2) substring match via lookup table
    for phrase, cat in _phrase_to_cat.items():
        if phrase in v:
            return cat
    return None

df["iris_cat_mapped"] = df["iris_cat"].apply(lookup_iris_category)

# Keep only the columns we actually need before concat to avoid schema mismatches
COMMON_COLS = ["date", "src_repo", "iris_cat_mapped"]

df_clean = df.loc[df["date"].notna() & df["iris_cat_mapped"].notna(), COMMON_COLS].copy()
swh_clean = swh.loc[swh["date"].notna(), COMMON_COLS].copy()

combined = pd.concat([df_clean, swh_clean], ignore_index=True)
combined["year"] = combined["date"].dt.year

years = sorted(y for y in combined["year"].dropna().unique().astype(int) if y >= 2004)
if not years:
    raise SystemExit("No valid years found in the dataset (>= 2004)")

# English display labels for the legend (maps Italian key → English display name)
iris_labels_en = {
    "7.01 Carta tematica e geografica":                        "7.01 Thematic and Geographic Map",
    "7.02 Carta geologica":                                    "7.02 Geological Map",
    "7.03 Prodotto dell'ingegneria civile e dell'architettura": "7.03 Civil Engineering / Architecture",
    "7.04 Software":                                           "7.04 Software",
    "7.05 Banche dati":                                        "7.05 Database",
    "7.06 Composizione musicale":                              "7.06 Musical Composition",
    "7.07 Disegno":                                            "7.07 Drawing",
    "7.08 Design":                                             "7.08 Design",
    "7.09 Performance":                                        "7.09 Performance",
    "7.10 Manufatto":                                          "7.10 Artefact",
    "7.11 Prototipo d'arte e relativi progetti":               "7.11 Art Prototype and Related Projects",
    "7.12 Mostra o Esposizione":                               "7.12 Exhibition",
    "7.13 Rapporto tecnico":                                   "7.13 Technical Report",
    "7.14 Audiovisivi":                                        "7.14 Audiovisual",
    "7.15 Test psicologici":                                   "7.15 Psychological Test",
}

PALETTE = [
    "#669900",  # Forest Moss
    "#99cc33",  # Yellow Green
    "#ccee66",  # Lime Cream
    "#006699",  # Baltic Blue
    "#3399cc",  # Blue Bell
    "#990066",  # Dark Raspberry
    "#cc3399",  # Medium Violet Red
    "#ff6600",  # Pumpkin Spice
    "#ff9900",  # Amber Glow
    "#ffcc00",  # Bright Amber
    "#336633",  # Deep Forest
    "#0099aa",  # Teal Bridge
    "#cc6633",  # Rust / Terracotta
    "#663366",  # Dark Plum
    "#ffee99",  # Pale Amber Cream
]

cat_colour = {cat: PALETTE[i % len(PALETTE)] for i, cat in enumerate(iris_categories)}

sources = [
    ("zenodo", "Zenodo"),
    ("amsacta", "AMS Acta"),
    ("iris", "IRIS"),
    ("software heritage", "Software Heritage"),
]

fig = make_subplots(
    rows=2,
    cols=2,
    subplot_titles=[display for _, display in sources],
    shared_xaxes=False,   # each subplot manages its own x-axis ticks
    vertical_spacing=0.18,
    horizontal_spacing=0.08,
)

# Shared axis style
AXIS_STYLE = dict(tickmode="linear", tick0=2004, dtick=1, tickangle=45)

for idx, (src_key, _) in enumerate(sources):
    row, col = divmod(idx, 2)
    row += 1
    col += 1

    df_src = combined[combined["src_repo"] == src_key]

    if src_key == "software heritage":
        # Single-bar series, no category breakdown needed
        counts = (
            df_src.groupby("year").size()
            .reindex(years, fill_value=0)
        )
        fig.add_trace(
            go.Bar(
                x=years,
                y=counts.values,
                name=iris_labels_en["7.04 Software"],
                marker_color=cat_colour["7.04 Software"],
                legendgroup="7.04 Software",
                showlegend=(idx == 0),
            ),
            row=row, col=col,
        )
    else:
        grouped = (
            df_src.groupby(["year", "iris_cat_mapped"])
            .size()
            .unstack(fill_value=0)
            .reindex(columns=iris_categories, fill_value=0)
            .reindex(index=years, fill_value=0)
        )
        for cat in iris_categories:
            fig.add_trace(
                go.Bar(
                    x=years,
                    y=grouped[cat].values,
                    name=iris_labels_en[cat],
                    marker_color=cat_colour[cat],
                    legendgroup=iris_labels_en[cat],
                    showlegend=(idx == 0),  # show legend only once
                ),
                row=row, col=col,
            )

    # Apply axis styles per subplot
    axis_idx = "" if idx == 0 else str(idx + 1)
    fig.update_layout(**{
        f"xaxis{axis_idx}": AXIS_STYLE,
        f"yaxis{axis_idx}": dict(range=[0, 650], autorange=False),
    })

fig.update_layout(
    barmode="stack",
    title_text="Timeline for Non-Traditional Research Outputs",
    legend_title_text="IRIS category",
    height=800,
)

#fig.show(renderer="browser")
pio.write_image(fig, "timegraph.jpeg", width=1400, height=900, scale=2)