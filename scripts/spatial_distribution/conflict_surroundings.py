import duckdb
import geopandas as gpd
from shapely.ops import transform
import shapely
import utility
import pyproj

gdf_land = gpd.read_file("../resources/boundaries/world.gpkg")
land_geom = gdf_land.geometry.union_all()

gdf_conflict_regions = gpd.read_file("../resources/conflict_regions/all.gpkg")
conflict_regions_geom = gdf_conflict_regions.geometry.union_all()

region_names = ("kaschmir", "karabach", "westbank", "westsahara", "gaza", "arunachal_pradesh", "transnistria")
for region_name in region_names:
    print(region_name)
    conflict_region: gpd.GeoDataFrame = gpd.read_file(f"../resources/conflict_regions/{region_name}/{region_name}.gpkg")
    conflict_region = conflict_region.geometry.union_all()

    crs = utility.local_crs_from_geom(conflict_region)
    transformer1 = pyproj.Transformer.from_crs(crs_from="EPSG:4326", crs_to=crs, always_xy=True)
    transformer2 = pyproj.Transformer.from_crs(crs_from=crs, crs_to="EPSG:4326", always_xy=True)
    conflict_region = transform(transformer1.transform, conflict_region)

    surrounding_area = 0
    i = 0
    while surrounding_area < conflict_region.area * 4:
        surrounding_geom: shapely.Polygon = conflict_region.buffer(1_000 * i)

        surrounding_geom = transform(transformer2.transform, surrounding_geom)
        surrounding_geom = surrounding_geom.intersection(land_geom)
        surrounding_geom_2 = surrounding_geom.intersection(conflict_regions_geom)
        surrounding_geom = surrounding_geom.symmetric_difference(surrounding_geom_2)
        surrounding_geom = transform(transformer1.transform, surrounding_geom)

        surrounding_area = surrounding_geom.area - surrounding_geom_2.area
        i += 1

    if region_name == "westsahara":
        surrounding_region = gpd.GeoDataFrame(data = [], geometry=list(surrounding_geom.geoms), crs = crs)
    else:
        surrounding_region = gpd.GeoDataFrame(data = [], geometry=[surrounding_geom], crs = crs)

    surrounding_region = surrounding_region.to_crs("EPSG:4326")
    surrounding_region.to_file(f"../resources/conflict_regions/{region_name}/{region_name}_surroundings.gpkg")