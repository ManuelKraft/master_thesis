import multiprocessing
import geopandas as gpd
import pandas as pd
import duckdb
import time
import os


def counting(file):
    df = con.sql(f"""WITH t1 AS (SELECT {tag_column} FROM read_parquet('{source_path}/{file}') as g1
                                WHERE ST_INTERSECTS(g1.geometry, ST_GeomFromText('{region_geom}'))
                                AND {osm_status_filter})
                    SELECT {select_statement} FROM t1""").df()
    print(f"Finished File: {file} ({time.strftime('%H:%M')})")
    return df
    
source_path = "../../ohsome/ohsome-parquet/103831"

con = duckdb.connect(database="../results/catched_deletions.db")
con.load_extension("spatial")

with open("../resources/osm_primary_features.txt", 'r') as file:
    primary_keys = [line.strip() for line in file]

BLOCK_SIZE = 500_000
osm_status_filter = ""

regions: gpd.GeoDataFrame = gpd.read_file("../resources/conflict_regions.gpkg")
for region in regions.layer.to_numpy():
    print(region)
    region_geom = regions[regions.layer == region].geometry.values[0]
    region_geom = region_geom.wkt

    df_full_total = pd.DataFrame()
    for osm_status in ["deleted", "visible"]:
        if osm_status == "deleted": 
            tag_column = "tags_before"
            osm_status_filter = """contrib_type = 'DELETED' AND NOT EXISTS(
                                SELECT * FROM catched_deletions as g2
                                WHERE g1.changeset_id = g2.changeset_id 
                                AND g1.osm_id = g2.osm_id)"""
            
        elif osm_status == "visible": 
            osm_status_filter = "visible = true"
            tag_column = "tags"

        select_statement = ""
        for key in primary_keys: 
            select_statement += f"COUNT(*) FILTER (JSON_EXTRACT({tag_column}, '$.{key}') IS NOT NULL) as {key}, "
        select_statement = select_statement[:-2]

        p = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 1)
        asyncs = []
        for k, file in enumerate(os.listdir(source_path)):
            a = p.apply_async(counting, args=(file,))
            asyncs.append(a)
        p.close()
        p.join()

        df_full = pd.DataFrame()
        for a in asyncs:
            df = a.get()
            df["osm_status"] = osm_status
            df_full = pd.concat([df_full, df])

        df_full_total = pd.concat([df_full_total, df_full], ignore_index=True)

    df_full_total = df_full_total.groupby("osm_status").sum()
    print(df_full_total)
    df_full_total.to_csv(f"../results/key_count_{region}_primary.csv")