import geopandas as gpd
import pandas as pd
import shapely

def has_six_sides(geom):
    if geom.geom_type == "Polygon":
        coords = list(geom.exterior.coords)
        if len(coords) - 1 == 6:
            return True
    return False

def has_approximately_equal_side_length(geom, tolerance=10_000):
    if geom.geom_type != "Polygon":
        return False

    coords = list(geom.exterior.coords)
    
    # Calculate lengths of all sides
    side_lengths = [
        ((coords[i][0] - coords[i + 1][0])**2 + (coords[i][1] - coords[i + 1][1])**2)**0.5
        for i in range(len(coords) - 1)
    ]
    
    # Check if all lengths are approximately equal (within the tolerance)
    mean_length = sum(side_lengths) / len(side_lengths)
    return all(abs((length/mean_length) - 1) <= 1 for length in side_lengths)

def is_hexagon(geom):
    if has_six_sides(geom) and has_approximately_equal_side_length(geom):
        return True
    return False

def get_missing_hexagons(gdf: gpd.GeoDataFrame) -> list[shapely.Polygon]: 
    shape = shapely.Polygon([(-170, 70), (170, 70), (170, -70), (-170, -70), (-170, 70)])
    hexagons_merged = gdf.geometry.unary_union
    return shape.symmetric_difference(hexagons_merged).geoms


print("Import...")
gdf = gpd.read_file("hexagons_11/dggrid.shp")
gdf = gdf.to_crs("EPSG:3857")

print("Hexagon Count before:", len(gdf))
gdf: gpd.GeoDataFrame = gdf[gdf["geometry"].apply(is_hexagon)]
print("Hexagon Count after:", len(gdf))

gdf = gdf.to_crs("EPSG:4326")

print("Remaining Hexagons...")
remaining_hexagons = get_missing_hexagons(gdf)
df = pd.DataFrame(gdf)
df2 = pd.DataFrame({"geometry": remaining_hexagons})
df_full = pd.concat([df.geometry, df2])
gdf = gpd.GeoDataFrame(data = df_full, geometry = df_full.geometry, crs = "EPSG:4326")

gdf = gdf.to_crs("EPSG:3857")

print("Hexagon Count before:", len(gdf))
max_area = max(gdf.geometry.area)
gdf: gpd.GeoDataFrame = gdf[gdf["geometry"].apply(lambda geom: geom.area < max_area)]
print("Hexagon Count after:", len(gdf))

gdf = gdf.to_crs("EPSG:4326")

print("Export...")
gdf.to_file("hexagons_11.gpkg")
print("Export Finished")