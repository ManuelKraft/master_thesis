from matplotlib import pyplot as plt
import pandas as pd
import numpy as np

df = pd.read_csv("../../results/object_differences/data/global/all.csv")
df = df.set_index("osm_status")
df_share = df.div(df.sum(axis = 1), axis=0)

df = df.rename(columns={"primary": "Gegenständlich", "address": "Nur Adressen", "empty": "Ohne Information" ,"other": "Sonstige"})
df = df[["Gegenständlich", "Nur Adressen", "Ohne Information", "Sonstige"]]
# Define x positions and bar width
columns = df.columns
x = np.arange(len(columns))  # One position for each column
bar_width = 0.35

# Plotting
fig, ax = plt.subplots(figsize=(12, 8))

# Create bars for 'visible' and 'deleted'
ax.bar(x - bar_width / 2, df.loc['visible'] / df.loc['visible'].sum(), bar_width, label='Anteil an allen sichtbaren Objekte', color='blue')
ax.bar(x + bar_width / 2, df.loc['deleted'] / df.loc['deleted'].sum(), bar_width, label='Anteil an allen Löschungen', color='orange')

ax.set_ylabel('Anteil an Gesamtobjekten', fontdict={"fontsize": 14})
ax.set_title('Häufigkeit von OSM-Objektgruppen (Weltweit)', fontdict={'fontsize': 18})
ax.set_xticks(x)
ax.set_xticklabels(columns, fontdict={"fontsize": 14})
ax.legend()
plt.savefig("../../results/object_differences/images/global/all.png")