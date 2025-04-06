import geopandas as gpd
import pandas as pd
import ast
from shapely import wkb

df = pd.read_csv("../results/object_differences/data/conflict_regions/westsahara/all_keys_berm.csv")
print(df.dtypes)

df.valid_from = pd.to_datetime(df.valid_from, utc=True)

df.changeset_tags = df.changeset_tags.apply(ast.literal_eval)
df["comments"] = df.changeset_tags.apply(lambda x: x.get('comment', None))
print(len(df))
#df = df.loc[(df.valid_from >= "2017-09-01") & (df.valid_from <= "2018-01-01")]
df2 = df[["valid_from", "tags_before", "comments", "changeset_id"]]
print(len(df2))
df2 = df.sort_values("changeset_id")
df2.to_csv("../results/object_differences/data/conflict_regions/westsahara/all_keys_berm_filter.csv")


