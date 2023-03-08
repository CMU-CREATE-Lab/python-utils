import shapely
import geopandas as gpd

def explore_geoms(geoms: list[shapely.geometry.base.BaseGeometry]):
    for geom in geoms:
        assert shapely.get_srid(geom) == 4326
    gdf = gpd.GeoDataFrame(geometry=geoms, crs=shapely.get_srid(geoms[0]))
    return gdf.explore()

def explore_geom(geom: shapely.geometry.base.BaseGeometry):
    return explore_geoms([geom])

def write_geom(geom: shapely.geometry.base.BaseGeometry, filename: str):
    assert shapely.get_srid(geom) == 4326
    gdf = gpd.GeoDataFrame(geometry=[geom], crs=shapely.get_srid(geom))
    gdf.to_file(filename, driver='GeoJSON')
    assert(gpd.read_file(filename).crs.to_epsg() == shapely.get_srid(geom))

def pgval(x):
    if isinstance(x, shapely.geometry.base.BaseGeometry):
        assert(shapely.get_srid(x) == 4326)
        return f"'{shapely.wkb.dumps(x, hex=True, include_srid=True)}'::geometry"
    elif isinstance(x, str):
        # TODO: properly quote this string
        return f"'{x}'"
    assert(False)

def pggeog(x):
    if isinstance(x, shapely.geometry.base.BaseGeometry):
        assert(shapely.get_srid(x) == 4326)
        return f"'{shapely.wkb.dumps(x, hex=True, include_srid=True)}'::geography"
    assert(False)

def pgarray(elts):
    return f"ARRAY[{','.join([pgval(elt) for elt in elts])}]"
    