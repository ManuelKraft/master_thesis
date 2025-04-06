from shapely import wkb
import multiprocessing
import duckdb
import pandas as pd
import os

def join(i, file):
    print(i, "started")
    results: list[tuple] = []
    con = duckdb.connect(database = f"../resources/temp_databases/temp_{i}.db")
    con.install_extension("spatial")
    con.load_extension("spatial")
    for offset in [0, CHUNK_SIZE]:
        con.sql(f"""CREATE TEMP TABLE t1 AS (
                    SELECT {select_statement}
                    FROM read_parquet('{source_path}/{file}')
                    WHERE {filter_statement}
                    AND ({primary_filter_statement})
                    OFFSET {offset} LIMIT {offset + CHUNK_SIZE})""")  
                    
        table = con.sql("SELECT * FROM t1")
        if len(table) == 0: break 

        con.sql("CREATE INDEX idx_1 ON t1 USING RTREE (geom);")

        for hex_id, hex_geom in hexagons.itertuples(index=False):
            hex_geom_string = hex_geom.wkt
            count = con.sql(f"""SELECT COUNT(*) as count1 FROM t1
                            WHERE ST_INTERSECTS('{hex_geom_string}', t1.geom)""").df().count1.values[0]
            if count > 0:
                results.append((hex_id, count))
            if hex_id % 50000 == 0:
                print(hex_id, i, offset)
        con.sql("DROP INDEX idx_1")
        con.sql("DROP TABLE t1")

    print(i, "finished")
    del con
    os.remove(f"../resources/temp_databases/temp_{i}.db")
    return results

OSM_OBJECTS_TO_JOIN = "visible_primary"
CHUNK_SIZE = 35_000_000

possible_osm_objects_to_join = ("visible", "visible_primary", "deletions", "deletions_primary")

if OSM_OBJECTS_TO_JOIN not in possible_osm_objects_to_join:
    raise ValueError(f"Invalid value: {OSM_OBJECTS_TO_JOIN}. Expected one of {possible_osm_objects_to_join}.")

if OSM_OBJECTS_TO_JOIN in ("deletions", "deletions_primary"):
    source_path = "../../ohsome/ohsome-parquet/103831-deletions_filtered"
    filter_statement = "contrib_type = 'DELETED"
    select_statement = "ST_GeomFromWKB(geometry) as geom"
    tag_column = "tags_before"
else:
    source_path = "../../ohsome/ohsome-parquet/103831"
    filter_statement = "visible = true"
    select_statement = "geometry as geom"
    tag_column = "tags"

if OSM_OBJECTS_TO_JOIN in ("visible_primary", "deletions_primary"):
    with open("../resources/osm_primary_features.txt", 'r') as file:
        primary_keys = [line.strip() for line in file]
    primary_filter_statement = ""
    for key in primary_keys: 
            primary_filter_statement += f"JSON_EXTRACT({tag_column}, '$.{key}') IS NOT NULL OR "
    primary_filter_statement = primary_filter_statement[:-4]
else:
    primary_filter_statement = "True"

con = duckdb.connect("../results/spatial_distribution.db")
con.load_extension("spatial")

hexagons = con.sql("SELECT id, ST_AsWkb(geom) as geom FROM hex_11").df()
hexagons_counter = {hex_id: 0 for hex_id in hexagons.id.values}
hexagons['geom'] = hexagons['geom'].apply(lambda x: wkb.loads(bytes(x)))


p = multiprocessing.Pool(processes=20)
asyncs = []
for i, file in enumerate(os.listdir(source_path)):
    a = p.apply_async(join, args=(i,file))
    asyncs.append(a)
p.close()
p.join()

for i, a in enumerate(asyncs):
    results = a.get()
    for hex_id, count in results:
        hexagons_counter[hex_id] += count

df = pd.DataFrame(data = {"id": hexagons_counter.keys(), "count": hexagons_counter.values()})

con.sql("CREATE TEMP TABLE t1 (id bigint, count bigint)")
con.register("df", df)
con.sql("INSERT INTO t1 SELECT * FROM df")

column_name = f"count_{OSM_OBJECTS_TO_JOIN}"
con.sql(f"ALTER TABLE hex_11 ADD COLUMN IF NOT EXISTS {column_name} bigint DEFAULT 0")
con.sql(f"UPDATE hex_11 SET {column_name} = t1.count FROM t1 WHERE hex_11.id = t1.id")