import duckdb

con = duckdb.connect(database="../results/spatial_distribution.db", read_only=True)
con.load_extension("spatial")

con.sql("ALTER TABLE hex_11 ADD COLUMN count_visible_per_100km² FLOAT DEFAULT 0")
con.sql("UPDATE hex_11 SET count_visible_per_100km² = (count_visible / 288) * 100")

con.sql("ALTER TABLE hex_11 ADD COLUMN count_deletions_per_100km² FLOAT DEFAULT 0")
con.sql("UPDATE hex_11 SET count_deletions_per_100km² = (count_deletions / 288) * 100")

con.sql("ALTER TABLE hex_11 ADD COLUMN count_visible_primary_per_100km² FLOAT DEFAULT 0")
con.sql("UPDATE hex_11 SET count_visible_primary_per_100km² = (count_visible_primary / 288) * 100")

con.sql("ALTER TABLE hex_11 ADD COLUMN count_deletions_primary_per_100km² FLOAT DEFAULT 0")
con.sql("UPDATE hex_11 SET count_deletions_primary_per_100km² = (count_deletions_primary / 288) * 100")

con.sql("ALTER TABLE hex_11 ADD COLUMN ratio FLOAT DEFAULT -999")
con.sql("UPDATE hex_11 SET ratio = count_deletions / count_visible WHERE count_visible >= 30")

con.sql("ALTER TABLE hex_11 ADD COLUMN ratio_primary FLOAT DEFAULT -999")
con.sql("UPDATE hex_11 SET ratio_primary = count_deletions_primary / count_visible_primary WHERE count_visible_primary >= 30")

con.sql("ALTER TABLE hex_11 ADD COLUMN land BOOLEAN DEFAULT false")

con.sql("""CREATE TEMP TABLE t2 AS (SELECT * FROM ST_Read('../resources/world.gpkg'));
            CREATE INDEX idx1 ON t2 USING RTree (geom)""")

con.sql("UPDATE hex_11 SET land = true FROM t2 WHERE ST_Intersects(hex_11.geom, t2.geom)")

con.sql("""CREATE TABLE hex_11_new AS 
        SELECT id, count_visible, count_deletions, count_visible_per_100km², count_deletions_per_100km², ratio
        count_visible_primary, count_deletions_primary, count_visible_primary_per_100km², count_deletions_primary_per_100km², ratio_primary, 
        land, geom FROM hex_11;""")

con.sql("""DROP TABLE hex_11; ALTER TABLE hex_11_new RENAME TO hex_11;""")