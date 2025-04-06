import geopandas as gpd
from shapely import wkb
import multiprocessing 
import warnings
import duckdb
import shapely
import utility
import time
import os

warnings.filterwarnings("ignore", message="invalid value encountered in hausdorff_distance")

def filter(changeset_id: int):
    con.sql(f"""DROP TABLE IF EXISTS temp_table; 
            CREATE TEMP TABLE temp_table AS 
            SELECT osm_id, changeset_id, contrib_type, tags, tags_before, geometry
            FROM read_parquet('{source_file}')
            WHERE changeset_id = {changeset_id}""")

    table = con.sql(f"""SELECT osm_id, contrib_type, tags, tags_before, ST_AsWkb(geometry) as geometry FROM temp_table
                    WHERE contrib_type in ('DELETED', 'CREATED')""").df()
    
    table['geometry'] = table['geometry'].apply(lambda x: wkb.loads(bytes(x)))
    table = utility.drop_invalid_geometries(table)

    deletions = table.loc[table.contrib_type == "DELETED"][["osm_id", "tags_before", "geometry"]]
    deletions = gpd.GeoDataFrame(deletions, geometry = "geometry", crs = "EPSG:4326")

    creations = table.loc[table.contrib_type == "CREATED"][["osm_id", "tags", "geometry"]]
    creations = gpd.GeoDataFrame(creations, geometry = "geometry", crs = "EPSG:4326")

    if len(deletions) == 0 or len(creations) == 0:
        return (changeset_id, [])
    
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

    return (changeset_id, osm_ids)


database = "../resources/catched_deletions.db"
source_file = "../../ohsome/ohsome-parquet/103831-order_by_changesets.parquet"

con = duckdb.connect(database = database, read_only = False)
con.load_extension("spatial")

THRESHOLD = 1
GROUP_SIZE = 1000
MAX_COMPUTATIONS = 5_000_000

remaining_changesets = con.sql(f"""SELECT * FROM changesets_to_filter t1
                        WHERE count_deletions >= {THRESHOLD} 
                        AND count_creations >= {THRESHOLD}
                        AND NOT EXISTS(SELECT * FROM changesets_filtered t2 
                                WHERE t1.changeset_id = t2.changeset_id)""").df().changeset_id

remaining_changesets_count = len(remaining_changesets)
print(f"Number of remaining Changesets to compute: {remaining_changesets_count} (Threshold: {THRESHOLD})\n")

if remaining_changesets_count > MAX_COMPUTATIONS:
        remaining_changesets = remaining_changesets[:MAX_COMPUTATIONS]
        remaining_changesets_count = MAX_COMPUTATIONS
        print(f"Reduced to {MAX_COMPUTATIONS} Changesets")


changeset_id_groups: list[int] = [remaining_changesets[i:i+GROUP_SIZE] for i in range(0, len(remaining_changesets), GROUP_SIZE)]

for changeset_id_group in changeset_id_groups:
    print(f"{remaining_changesets_count} ({time.strftime('%H:%M')})")
    p = multiprocessing.Pool(processes=(multiprocessing.cpu_count() - 1))

    asyncs = []
    for changeset_id in changeset_id_group:
        a = p.apply_async(filter, args=(changeset_id,))
        asyncs.append(a)
    p.close()
    p.join()

    for a in asyncs:
        changeset_id, osm_ids = a.get()
        if len(osm_ids) > 0:
            con.executemany("INSERT INTO catched_deletions VALUES (?, ?)", [(str(osm_id), int(changeset_id)) for osm_id in osm_ids])
        con.sql(f"INSERT INTO changesets_filtered VALUES ({changeset_id})")

    remaining_changesets_count -= GROUP_SIZE
