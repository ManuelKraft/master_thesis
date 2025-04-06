import duckdb
import os

SOURCE_PATH = "../../ohsome/ohsome-parquet/103831"
DESTINATION_PATH = "../../ohsome/ohsome-parquet/103831-deletions_filtered"
con = duckdb.connect(database = "../results/catched_deletions.db")


for i, file in enumerate(os.listdir(SOURCE_PATH)):
    print(i)
    con.sql(f"""COPY (SELECT tags_before, geometry FROM read_parquet('{SOURCE_PATH}/{file}') t1
                        WHERE contrib_type = 'DELETED'
                        AND NOT EXISTS (
                                SELECT * FROM catched_deletions g1
                                WHERE t1.osm_id = g1.osm_id 
                                AND t1.changeset_id = g1.changeset_id)
                        ) TO '{DESTINATION_PATH}/{file}' (FORMAT PARQUET)""")


