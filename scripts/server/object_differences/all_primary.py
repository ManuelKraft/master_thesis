import multiprocessing
import pandas as pd
import duckdb
import time
import os


def counting(i):
    return con.sql(f"""WITH t1 AS (SELECT {tag_column} FROM read_parquet('{source_path}/{file}') as g1
                                WHERE {osm_status_filter} OFFSET {i} LIMIT {BLOCK_SIZE})
                    SELECT {select_statement} FROM t1""").df()
    
source_path = "../../ohsome/ohsome-parquet/103831"
con = duckdb.connect(database="../resources/catched_deletions.db")

with open("../resources/osm_primary_features.txt", 'r') as file:
    primary_keys = [line.strip() for line in file]

BLOCK_SIZE = 500_000

osm_status_filter = ""
        
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

    asyncs = []
    for k, file in enumerate(os.listdir(source_path)):
        total_count =  len(con.sql(f"SELECT * FROM read_parquet('{source_path}/{file}') as g1 WHERE {osm_status_filter}"))
        p = multiprocessing.Pool(processes=(multiprocessing.cpu_count() - 1))
        for i in range(0, total_count, BLOCK_SIZE):
            a = p.apply_async(counting, args=(i,))
            asyncs.append(a)
        p.close()
        p.join()
        print(f"Finished File: {file} ({time.strftime('%H:%M')})")

    df_full = pd.DataFrame()
    for a in asyncs:
        df = a.get()
        df["osm_status"] = osm_status
        df_full = pd.concat([df_full, df])

    df_full_total = pd.concat([df_full_total, df_full], ignore_index=True)

print(df_full_total)
df_full_total.to_csv("../results/key_count_all_primary.csv")