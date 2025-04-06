from matplotlib import pyplot as plt
import geopandas as gpd
import pandas as pd

reason = "local_highlights"
region = "australia"
key = "all_keys"

deletions: pd.DataFrame = pd.read_csv(f"../results/object_differences/data/{reason}/{region}/{key}.csv")
deletions.valid_from = pd.to_datetime(deletions.valid_from, utc=True)
deletions.valid_from = deletions.valid_from.dt.to_period('M')
deletions = deletions.valid_from.value_counts().sort_index()
deletions = deletions.reset_index()
deletions = deletions.rename(columns={"valid_from": "timestamp"})
deletions = deletions.sort_values(["timestamp", "count"], ascending=[True, False]).drop_duplicates(subset="timestamp", keep="first")

visible: pd.DataFrame = pd.read_csv(f"../results/object_differences/data/{reason}/{region}/{key}_visible.csv")
visible.timestamp = pd.to_datetime(visible.timestamp, utc=True)
visible.timestamp = visible.timestamp.dt.to_period('M')
visible = visible.sort_values(["timestamp", "value"], ascending=[True, False]).drop_duplicates(subset="timestamp", keep="first")

if region in ("westbank", "southafrica_cluster"):
    visible_hourly: pd.DataFrame = pd.read_csv(f"../results/object_differences/data/{reason}/{region}/{key}_visible_hourly.csv")
    visible_hourly.timestamp = pd.to_datetime(visible_hourly.timestamp, utc=True)
    visible_hourly.timestamp = visible_hourly.timestamp.dt.to_period('M')
    dfh = visible_hourly.sort_values(["timestamp", "value"], ascending=[True, False]).drop_duplicates(subset="timestamp", keep="first")
    visible = pd.concat([visible_hourly, visible])

# Keep only the row with the highest 'value' for each timestamp
visible = visible.loc[visible.groupby("timestamp")["value"].idxmax()]


df = visible.merge(deletions, on='timestamp', how='left')
df = df.rename(columns={"value": "Sichtbar", "count": "Gelöscht"})

df["timestamp"] = pd.to_datetime(df["timestamp"].astype(str))
df.to_csv(f"../results/object_differences/data/{reason}/{region}/deletion_visible_time_series.csv")

shift = pd.Timedelta(days=10) 
min_timestamp = "2009-01-01"
max_timestamp = "2018-01-01"
df = df[(df.timestamp > min_timestamp) & (df.timestamp < max_timestamp)]
width = 20

if (df.Sichtbar.max() > df.Gelöscht.max() * 5) or (df.Sichtbar.max() < df.Gelöscht.max() / 5):
    fig, ax1 = plt.subplots(figsize=(12, 6))

    ax1.bar(df["timestamp"], df["Sichtbar"], width=width, label="Sichtbar", color="blue")
    ax1.set_ylabel("Sichtbar", color="blue")
    ax1.tick_params(axis='y', labelcolor="blue")
    ax1.grid(True, linestyle="--", alpha=0.5)

    ax2 = ax1.twinx()
    ax2.bar(df["timestamp"] + shift, df["Gelöscht"], width=width, label="Gelöscht", color="red")
    ax2.set_ylabel("Gelöscht", color="red")
    ax2.tick_params(axis='y', labelcolor="red")
else:
    plt.figure(figsize=(12, 6))
    plt.bar(df.timestamp, df["Sichtbar"], width=width, label="Sichtbar", color="blue")
    plt.bar(df.timestamp + shift, df["Gelöscht"], width=width, label="Gelöscht", color="red")

plt.legend(loc="upper left", bbox_to_anchor=(0, 1))
plt.xlim(pd.Timestamp(min_timestamp), pd.Timestamp(max_timestamp))
plt.title(f"Sichtbare vs. Gelöschte Objekte", fontdict={"fontsize": 16}, pad=30)
plt.subplots_adjust(top=0.85)
plt.text(0.5, 0.875, "Australische Highways mit besonders vielen Löschungen", fontsize=12,  ha="center", transform=plt.gcf().transFigure)
plt.savefig(f"../results/object_differences/images/{reason}/{region}/deletion_visible_time_series_subset.png")