import duckdb

def create_table_for_changesets_to_filter(con: duckdb.DuckDBPyConnection):
    source_file = "../ohsome/ohsome-parquet/103831-order_by_changesets.parquet"
    con.sql("DROP TABLE IF EXISTS changesets_to_filter")
    con.sql(f"""CREATE TABLE IF NOT EXISTS changesets_to_filter AS
                    SELECT changeset_id, 
                    COUNT(*) FILTER (contrib_type = 'DELETED') as count_deletions, 
                    COUNT(*) FILTER (contrib_type = 'CREATED') as count_creations
                    FROM read_parquet('{source_file}')
                    WHERE ST_IsValid(geometry)
                    GROUP BY changeset_id 
                    HAVING COUNT(*) FILTER (contrib_type = 'DELETED') >= 1
                    AND COUNT(*) FILTER (contrib_type = 'CREATED') >= 1""")

def create_table_for_filtered_changesets(con: duckdb.DuckDBPyConnection):
    con.sql("CREATE TABLE IF NOT EXISTS changesets_filtered(changeset_id bigint)")

def create_table_for_catched_deletions(con: duckdb.DuckDBPyConnection):
    con.sql("CREATE TABLE IF NOT EXISTS catched_deletions(osm_id VARCHAR(20), changeset_id bigint)")


database = "catch_deletions.db"
con = duckdb.connect(database = database, read_only = False)
con.load_extension("spatial")

create_table_for_changesets_to_filter(con=con)
#create_table_for_filtered_changesets(con=con)
#create_table_for_catched_deletions(con=con)


