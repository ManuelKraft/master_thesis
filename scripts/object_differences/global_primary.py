from matplotlib import pyplot as plt
import pandas as pd
import numpy as np

df: pd.DataFrame = pd.read_csv("../../results/object_differences/data/global/primary.csv")
df = df.set_index("osm_status")

df.loc["deleted / visible"] = round(df.iloc[0] / df.iloc[1], 3)
df.loc["deleted_share"] = df.loc["deleted"] / df.loc["deleted"].sum()
df.loc["visible_share"] = df.loc["visible"] / df.loc["visible"].sum()

df = df.loc[:, (df.loc["visible_share"] > 0.01) | (df.loc["deleted_share"] > 0.1) | (df.loc["visible"] > 3000)]
df = df.sort_values(by=["deleted / visible"], axis=1, ascending=False)
mean = df.loc["deleted"].sum() / df.loc["visible"].sum()

# Select the third row for plotting
row_to_plot = df.loc["deleted / visible"]
fig, ax = plt.subplots(figsize=(14, 18))
row_to_plot.plot(kind='bar', color='b')
ax.set_title("Löschungen von gegenständlichen Objekten (Weltweit)", fontdict={"fontsize": 24})

# Label the axes
ax.set_ylabel("Gelöschte Objekte / Sichtbare Objekte", fontdict={"fontsize": 22})
ax.tick_params(axis='y', labelsize=16) 
columns = df.columns
ax.set_xticklabels(columns, fontdict={"fontsize": 16})
ax.hlines(y = mean, xmin=-1, xmax=30, linestyles="dashed", colors="darkred")
ax.text(x = 20, y = mean + 0.02, s = f"Mittelwert \u2248 {mean:.3f}", color="darkred", va="center", ha="left", fontdict={"fontsize": 18})
plt.savefig("../../results/object_differences/images/global/primary.png")

tall_part = df.loc[:, (df.loc["visible_share"] > 0.005) | (df.loc["deleted_share"] > 0.01)]
small_part = df = df.loc[:, (df.loc["visible_share"] < 0.005) & (df.loc["deleted_share"] < 0.01)]

for df in (tall_part, small_part):
    if df.equals(tall_part): 
        out_path = "../../results/object_differences/images/global/primary_tall_shares.png"

    else: out_path = "../../results/object_differences/images/global/primary_small_shares.png"

    fig, ax = plt.subplots(figsize=(12, 8))
    df = df.sort_values(by=["deleted_share"], axis=1, ascending=False)
    columns = df.columns
    x = np.arange(len(columns))
    bar_width = 0.35

    # Create bars for 'visible' and 'deleted'
    ax.bar(x - bar_width / 2, df.loc["deleted_share"], bar_width, label='Anteil an allen Löschungen', color='orange')
    ax.bar(x + bar_width / 2, df.loc["visible_share"], bar_width, label='Anteil an allen sichtbaren Objekte', color='blue')

    ax.set_ylabel('Anteil an Gesamtobjekten')
    ax.set_title('Häufigkeiten von gegenständlichen Objekten (Weltweit)')
    ax.set_xticks(x)
    ax.set_xticklabels(columns, rotation=45)
    ax.legend()
    plt.savefig(out_path)