import geopandas as gpd
from shapely import wkb
import shapely
import utility
import duckdb
import time

database = "../resources/catched_deletions.db"

con = duckdb.connect(database = database, read_only = True)
con.load_extension("spatial")

source_file = "../../ohsome/ohsome-parquet/103831-order_by_changesets.parquet"
THRESHOLD = 1

remaining_changesets = con.sql(f"""SELECT * FROM changesets_to_filter t1
                        WHERE count_deletions >= {THRESHOLD} 
                        AND count_creations >= {THRESHOLD}
                        AND NOT EXISTS(SELECT * FROM changesets_filtered t2 
                                WHERE t1.changeset_id = t2.changeset_id)""").df().changeset_id

remaining_changesets_count = len(remaining_changesets)
print(f"Number of remaining Changesets to compute: {remaining_changesets_count} (Threshold: {THRESHOLD})\n")

MAX_COMPUTATIONS = 1_000_000
if remaining_changesets_count > MAX_COMPUTATIONS:
        remaining_changesets = remaining_changesets[:MAX_COMPUTATIONS]
        remaining_changesets_count = MAX_COMPUTATIONS
        print(f"Reduced to {MAX_COMPUTATIONS} Changesets")

for changeset_id in remaining_changesets:
    start = time.time()
    remaining_changesets_count -= 1

    con.sql(f"""DROP TABLE IF EXISTS temp_table; 
            CREATE TEMP TABLE temp_table AS 
            SELECT osm_id, changeset_id, contrib_type, tags, tags_before, geometry
            FROM read_parquet('{source_file}')
            WHERE changeset_id = {changeset_id}""")
    
    table = con.sql("""SELECT osm_id, contrib_type, tags, tags_before, ST_AsWkb(geometry) as geometry FROM temp_table
                    WHERE contrib_type in('DELETED', 'CREATED')""").df()
    
    table['geometry'] = table['geometry'].apply(lambda x: wkb.loads(bytes(x)))
    table = utility.drop_invalid_geometries(table)


    deletions = table.loc[table.contrib_type == "DELETED"][["osm_id", "tags_before", "geometry"]]
    deletions = gpd.GeoDataFrame(deletions, geometry = "geometry", crs = "EPSG:4326")

    creations = table.loc[table.contrib_type == "CREATED"][["osm_id", "tags", "geometry"]]
    creations = gpd.GeoDataFrame(creations, geometry = "geometry", crs = "EPSG:4326")

    if len(deletions) == 0 or len(creations) == 0:
        con.execute(f"INSERT INTO changesets_filtered VALUES ({changeset_id})")
        continue
        
    deletions_convex_hull = shapely.convex_hull(shapely.union_all(deletions.geometry))
    crs_objects = utility.possible_crs_objects(geometry = deletions_convex_hull)

    multiple_utm_zones = True
    if len(crs_objects) == 1:
            multiple_utm_zones = False
            crs_code = crs_objects[0].code
            deletions.to_crs(crs_code)
            creations.to_crs(crs_code)
            deletions = utility.drop_invalid_geometries(deletions)
            creations = utility.drop_invalid_geometries(creations)

    osm_ids = utility.filter_deletions(deletions=deletions, creations=creations, multiple_utm_zones=multiple_utm_zones)
    if len(osm_ids) > 0:
        con.executemany("INSERT INTO catched_deletions VALUES (?, ?)", [(osm_id, changeset_id) for osm_id in osm_ids])
    con.execute(f"INSERT INTO changesets_filtered VALUES ({changeset_id})")
    
    print(f"{(len(osm_ids) / len(deletions))*100:.1f}% Deletions catched ({len(deletions)} x {len(creations)}),"
                      f" {multiple_utm_zones}, {remaining_changesets_count}, {time.strftime('%H:%M')}")
