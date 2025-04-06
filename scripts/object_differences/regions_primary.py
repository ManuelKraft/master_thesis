from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import os

regions = os.listdir("../../results/object_differences/data/local_highlights")

for region in regions:
    region_os = region.replace(" ", "_").lower()
    df: pd.DataFrame = pd.read_csv(f"../../results/object_differences/data/local_highlights/{region_os}/primary.csv")
    df = df.set_index("osm_status")
    if region == "Westbank": region = "Westjordanland"

    #Create first Plot
    df.loc["deleted / visible"] = round(df.iloc[0] / df.iloc[1], 3)
    df.loc["deleted_share"] = df.loc["deleted"] / df.loc["deleted"].sum()
    df.loc["visible_share"] = df.loc["visible"] / df.loc["visible"].sum()

    df = df.loc[:, (df.loc["visible"] > 2000) | (df.loc["deleted"] > 4000)]
    df = df.sort_values(by=["deleted / visible"], axis=1, ascending=False)
    mean = df.loc["deleted"].sum() / df.loc["visible"].sum()

    row_to_plot = df.loc["deleted / visible"]      
    fig, ax = plt.subplots(figsize=(14, 18))
    row_to_plot.plot(kind='bar', color='b')
    ax.set_title(f"Löschungen von gegenständlichen Objekten ({region})", fontdict={"fontsize": 24})

    ax.set_ylabel("Gelöschte Objekte / Sichtbare Objekte", fontdict={"fontsize": 22})
    ax.tick_params(axis='y', labelsize=16) 
    columns = df.columns
    ax.set_xticklabels(columns, fontdict={"fontsize": 16})
    ax.hlines(y = mean, xmin=-1, xmax=30, linestyles="dashed", colors="darkred")
    ax.text(x = len(columns) - 3, y = mean + 0.02, s = f"Mittelwert \u2248 {mean:.3f}", color="darkred", va="center", ha="left", fontdict={"fontsize": 18})
    plt.savefig(f"../../results/object_differences/images/local_highlights/{region_os}/primary.png")

    #Create second Plot
    fig, ax = plt.subplots(figsize=(12, 8))
    df = df.sort_values(by=["deleted_share"], axis=1, ascending=False)
    columns = df.columns
    x = np.arange(len(columns))
    bar_width = 0.35

    ax.bar(x - bar_width / 2, df.loc["deleted_share"], bar_width, label='Anteil an allen Löschungen', color='orange')
    ax.bar(x + bar_width / 2, df.loc["visible_share"], bar_width, label='Anteil an allen sichtbaren Objekte', color='blue')

    ax.set_ylabel('Anteil an Gesamtobjekten')
    ax.set_title(f'Häufigkeit von gegenständlichen Objekten ({region})', fontdict={'fontsize': 16})
    ax.set_xticks(x)
    ax.set_xticklabels(columns, rotation=45)
    ax.legend()
    plt.savefig(f"../../results/object_differences/images/local_highlights/{region_os}/primary_shares.png")