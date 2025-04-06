from pyproj.database import query_utm_crs_info, CRSInfo
from pyproj.aoi import AreaOfInterest, AreaOfUse
from pyproj.enums import PJType
import geopandas as gpd
from pyproj import CRS
import numpy as np
import shapely
import rtree

def possible_crs_objects(geometry) -> list[CRSInfo]:
    min_x, min_y, max_x, max_y = geometry.bounds    
    crs_info_list = query_utm_crs_info(
        datum_name="WGS 84",
        area_of_interest=AreaOfInterest(
            west_lon_degree=min_x,
            south_lat_degree=min_y,
            east_lon_degree=max_x,
            north_lat_degree=max_y))

    #handle polar regions (because they are not suitable for utm projection)
    if min_y <= -80: 
        antarctic_crs_info = CRSInfo(auth_name='EPSG', code='3031', deprecated=False,
                        name='WGS 84 / Antarctic Polar Stereographic', type = PJType.PROJECTED_CRS,
                        area_of_use=AreaOfUse(west=-180.0, south=-90.0, east=180.0, north=-80.0, name='Antarctica'),
                        projection_method_name="polar Stereographic (variant B)")
        crs_info_list.append(antarctic_crs_info)
    elif max_y >= 80: 
        arctic_crs_info = CRSInfo(auth_name='EPSG', code='3995', deprecated=False,
                name='blabla', type = PJType.PROJECTED_CRS,
                area_of_use=AreaOfUse(west=-180.0, south=90.0, east=180.0, north=80.0, name='Arctic'),
                projection_method_name="blabla")
        crs_info_list.append(arctic_crs_info)

    return crs_info_list


def local_crs_with_highest_intersection(geometry: shapely.MultiPolygon, crs_info_list) -> str:
    max_area = 0
    crs_info_final = crs_info_list[0]
    for crs_info in crs_info_list:
        crs = CRS.from_epsg(crs_info.code)
        crs_bounds = crs.area_of_use.bounds
        crs_area = shapely.box(*crs_bounds)
        overlapping_area = crs_area.intersection(geometry).area
        if overlapping_area > max_area:
            max_area = overlapping_area
            crs_info_final = crs_info
    return crs_info_final.code


def local_crs_from_geom(geometry) -> str:
    crs_info_list = possible_crs_objects(geometry)
    if len(crs_info_list) > 1:
        return local_crs_with_highest_intersection(geometry, crs_info_list)
    else:
        return crs_info_list[0].code


def spatial_index(creations: gpd.GeoDataFrame):
    idx = rtree.index.Index()
    for pos, creation in enumerate(creations.itertuples(index=False)):
        idx.insert(pos, creation.geometry.bounds)
    return idx


def filter_deletions(deletions: gpd.GeoDataFrame, creations: gpd.GeoDataFrame, multiple_utm_zones: bool):
    catched_creations = set()
    catched_deletions = np.empty(len(deletions), dtype='U18')
    idx = spatial_index(creations=creations)
    
    j = 0
    if multiple_utm_zones:
        for deletion in deletions.itertuples(index=False):
            crs_code = local_crs_from_geom(deletion.geometry)
            if crs_code != deletions.crs:
                deletions = deletions.to_crs(crs_code)
                creations = creations.to_crs(crs_code)
                deletions = drop_invalid_geometries(deletions)
                creations = drop_invalid_geometries(creations)
                if len(deletions) == 0 or len(creations) == 0: break
                idx = spatial_index(creations=creations)

            possible_creations = list(idx.nearest(deletion.geometry.bounds, 10))
            for creation_pos in possible_creations:
                creation = creations.iloc[creation_pos]
                if creation.osm_id in catched_creations: continue
                distance = shapely.hausdorff_distance(deletion.geometry, creation.geometry)
                if distance < 30 and deletion.tags_before == creation.tags:
                    catched_creations.add(creation.osm_id)
                    catched_deletions[j] = deletion.osm_id
                    j += 1
                    break
    
    else:
        for deletion in deletions.itertuples(index=False):
            possible_creations = list(idx.nearest(deletion.geometry.bounds, 10))
            for creation_pos in possible_creations:
                creation = creations.iloc[creation_pos]
                if creation.osm_id in catched_creations: continue
                distance = shapely.hausdorff_distance(deletion.geometry, creation.geometry)
                if distance < 30 and deletion.tags_before == creation.tags:
                    catched_creations.add(creation.osm_id)
                    catched_deletions[j] = deletion.osm_id
                    j += 1
                    break

    return catched_deletions[:j]


def drop_invalid_geometries(gdf: gpd.GeoDataFrame):
    gdf = gdf[gdf["geometry"].apply(lambda geom: geom.is_valid if geom else False)]
    return gdf.reset_index(drop=True)
