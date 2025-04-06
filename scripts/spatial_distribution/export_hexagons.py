import geopandas as gpd
from shapely import wkb
import duckdb

con = duckdb.connect(database = "../results/spatial_distribution.db", read_only=True)
con.load_extension("spatial")

df = con.sql("SELECT * REPLACE (ST_AsWKB(geom) AS geom) FROM hex_11").df()
df.geom = df.geom.apply(lambda x: wkb.loads(bytes(x)))

df_without_geom = df.drop(columns=['geom'])
gpd.GeoDataFrame(data = df_without_geom, geometry=df.geom, crs="EPSG:4326").to_file("../results/hex_11.gpkg")





