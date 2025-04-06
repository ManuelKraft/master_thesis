import duckdb
import pandas as pd
import eb_smoother as eb

con = duckdb.connect(database="../results/spatial_distribution.db")
con.load_extension("spatial")

con.sql("ALTER TABLE hex_11 ADD COLUMN IF NOT EXISTS count_visible_per_100km² FLOAT DEFAULT 0")
con.sql("UPDATE hex_11 SET count_visible_per_100km² = (count_visible / 288) * 100")

con.sql("ALTER TABLE hex_11 ADD COLUMN IF NOT EXISTS count_deletions_per_100km² FLOAT DEFAULT 0")
con.sql("UPDATE hex_11 SET count_deletions_per_100km² = (count_deletions / 288) * 100")

con.sql("ALTER TABLE hex_11 ADD COLUMN IF NOT EXISTS count_visible_primary_per_100km² FLOAT DEFAULT 0")
con.sql("UPDATE hex_11 SET count_visible_primary_per_100km² = (count_visible_primary / 288) * 100")

con.sql("ALTER TABLE hex_11 ADD COLUMN IF NOT EXISTS count_deletions_primary_per_100km² FLOAT DEFAULT 0")
con.sql("UPDATE hex_11 SET count_deletions_primary_per_100km² = (count_deletions_primary / 288) * 100")

con.sql("ALTER TABLE hex_11 ADD COLUMN IF NOT EXISTS ratio FLOAT DEFAULT -999")
con.sql("UPDATE hex_11 SET ratio = count_deletions / count_visible WHERE count_visible >= 30")

con.sql("ALTER TABLE hex_11 ADD COLUMN IF NOT EXISTS ratio_primary FLOAT DEFAULT -999")
con.sql("UPDATE hex_11 SET ratio_primary = count_deletions_primary / count_visible_primary WHERE count_visible_primary >= 30")

#con.sql("ALTER TABLE hex_11 ADD COLUMN IF NOT EXISTS land BOOLEAN DEFAULT false")
#con.sql("""CREATE TEMP TABLE t2 AS (SELECT * FROM ST_Read('../resources/boundaries/world.gpkg'));
#            CREATE INDEX idx1 ON t2 USING RTree (geom)""")
#con.sql("UPDATE hex_11 SET land = true FROM t2 WHERE ST_Intersects(hex_11.geom, t2.geom)")

con.sql("ALTER TABLE hex_11 ADD COLUMN IF NOT EXISTS ratio_smoothed FLOAT DEFAULT -999")
df = con.sql("SELECT id, count_visible, count_deletions, ratio FROM hex_11 WHERE ratio != -999").df()
mean = eb.prior_mean(nominators=df.count_deletions, denominators=df.count_visible)
variance = eb.prior_variance(prior_mean=mean, nominators=df.count_deletions, denominators=df.count_visible)
weights = eb.prior_distribution_weights(prior_mean=mean, prior_variance=variance, denominators=df.count_visible)
ratios_smoothed = eb.eb_estimates(weights=weights, proportions=df.ratio, prior_mean=mean)
ratios_smoothed = pd.DataFrame({"hex_id": df.id, "ratio_smoothed": ratios_smoothed}, index=None)
con.sql("CREATE TEMP TABLE t1 (id bigint, ratio_smoothed FLOAT)")
con.register("ratios_smoothed", ratios_smoothed)
con.sql("INSERT INTO t1 SELECT * FROM ratios_smoothed")
con.sql(f"UPDATE hex_11 SET ratio_smoothed = t1.ratio_smoothed FROM t1 WHERE hex_11.id = t1.id; DROP TABLE t1")

con.sql("ALTER TABLE hex_11 ADD COLUMN IF NOT EXISTS ratio_smoothed_primary FLOAT DEFAULT -999")
df = con.sql("SELECT id, count_visible_primary, count_deletions_primary, ratio_primary FROM hex_11 WHERE ratio_primary != -999").df()
mean = eb.prior_mean(nominators=df.count_deletions_primary, denominators=df.count_visible_primary)
variance = eb.prior_variance(prior_mean=mean, nominators=df.count_deletions_primary, denominators=df.count_visible_primary)
weights = eb.prior_distribution_weights(prior_mean=mean, prior_variance=variance, denominators=df.count_visible_primary)
ratios_smoothed_primary = eb.eb_estimates(weights=weights, proportions=df.ratio_primary, prior_mean=mean)
ratios_smoothed_primary = pd.DataFrame({"hex_id": df.id, "ratio_smoothed_primary": ratios_smoothed_primary}, index=None)
con.sql("CREATE TEMP TABLE t1 (id bigint, ratio_smoothed_primary FLOAT)")
con.register("ratios_smoothed_primary", ratios_smoothed_primary)
con.sql("INSERT INTO t1 SELECT * FROM ratios_smoothed_primary")
con.sql(f"UPDATE hex_11 SET ratio_smoothed_primary = t1.ratio_smoothed_primary FROM t1 WHERE hex_11.id = t1.id")

con.sql("""CREATE TABLE hex_11_new AS 
        SELECT id, count_visible, count_deletions, count_visible_per_100km², count_deletions_per_100km², ratio, ratio_smoothed,
        count_visible_primary, count_deletions_primary, count_visible_primary_per_100km², count_deletions_primary_per_100km², 
        ratio_primary, ratio_smoothed_primary, land, geom FROM hex_11;""")

con.sql("""DROP TABLE hex_11; ALTER TABLE hex_11_new RENAME TO hex_11;""")