import multiprocessing
import pandas as pd
import duckdb
import time
import os


def counting(i):
    return con.sql(f"""WITH t1 AS (SELECT * FROM read_parquet('{source_path}/{file}') as g1
                                WHERE {osm_status_filter} OFFSET {i} LIMIT {BLOCK_SIZE})
                    SELECT 
                    COUNT(*) FILTER ({primary_key_condition}) as primary,
                    COUNT(*) FILTER ({address_condition}) as address,
                    COUNT(*) FILTER ({other_condition}) as other,
                    COUNT(*) FILTER ({empty_condition}) as empty
                    FROM t1""").df()
    

source_path = "../../ohsome/ohsome-parquet/103831"

con = duckdb.connect(database="../resources/catched_deletions.db")

with open("osm_primary_features.txt", 'r') as file:
    primary_keys = [line.strip() for line in file]


address_key = "addr"

total_count_primary = total_count_addresses = total_count_other = total_count_empty = 0
BLOCK_SIZE = 100_000

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

    primary_key_condition = ""
    for key in primary_keys: primary_key_condition += f"JSON_EXTRACT({tag_column}, '$.{key}') IS NOT NULL OR "

    primary_key_condition = primary_key_condition[:-4]
    address_condition = f"NOT ({primary_key_condition}) AND CAST({tag_column} AS VARCHAR) LIKE '%{address_key}:%'"
    other_condition = f"NOT ({primary_key_condition}) AND NOT (CAST({tag_column} AS VARCHAR) LIKE '%{address_key}:%') AND cardinality({tag_column}) > 0"
    empty_condition = f"cardinality({tag_column}) = 0"

    df_full = pd.DataFrame()
    for k, file in enumerate(os.listdir(source_path)):
        total_count =  len(con.sql(f"SELECT * FROM read_parquet('{source_path}/{file}') as g1 WHERE {osm_status_filter}"))
        p = multiprocessing.Pool(processes=(multiprocessing.cpu_count() - 1))
        asyncs = []
        for i in range(0, total_count, BLOCK_SIZE):
            a = p.apply_async(counting, args=(i,))
            asyncs.append(a)
        p.close()
        p.join()

        for a in asyncs:
            df = a.get()
            df["osm_status"] = osm_status
            df_full = pd.concat([df_full, df])

        print(f"Finished File: {file} ({time.strftime('%H:%M')})")

    df_full_total = pd.concat([df_full_total, df_full], ignore_index=True)

print(df_full_total)
df_full_total.to_csv("../results/key_count_all.csv")