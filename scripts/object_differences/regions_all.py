from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import os

regions = os.listdir("../../results/object_differences/data/local_highlights")

for region in regions:
    region_os = region.replace(" ", "_").lower()
    df = pd.read_csv(f"../../results/object_differences/data/local_highlights/{region_os}/all.csv")
    if region == "Westbank": region = "Westjordanland"

    df = df.set_index("osm_status")
    df_share = df.div(df.sum(axis = 1), axis=0)

    df = df.rename(columns={"primary": "Gegenständlich", "address": "Nur Adressen", "empty": "Ohne Information" ,"other": "Sonstige"})
    df = df[["Gegenständlich", "Nur Adressen", "Ohne Information", "Sonstige"]]

    #Create Plot
    fig, ax = plt.subplots(figsize=(12, 8))
    columns = df.columns
    x = np.arange(len(columns))
    bar_width = 0.35

    ax.bar(x - bar_width / 2, df.loc['visible'] / df.loc['visible'].sum(), bar_width, label='Anteil an allen sichtbaren Objekte', color='blue')
    ax.bar(x + bar_width / 2, df.loc['deleted'] / df.loc['deleted'].sum(), bar_width, label='Anteil an allen Löschungen', color='orange')

    ax.set_ylabel('Anteil an Gesamtobjekten', fontdict={"fontsize": 14})
    ax.set_title(f'Vergleich von OSM-Objektgruppen ({region})', fontdict={'fontsize': 18})
    ax.set_xticks(x)
    ax.set_xticklabels(columns, fontdict={"fontsize": 14})
    ax.legend()
    plt.savefig(f"../../results/object_differences/images/local_highlights/{region_os}/all.png")