import io, subprocess
import geopandas as gpd

def read_kml_using_mapbox(kml_pathname: str, npx_path: str = "npx") -> gpd.GeoDataFrame:
    print(f"Converting {kml_pathname} to json using mapbox/togeojson")
    cmd = f"{npx_path} togeojson '{kml_pathname}'"
    print(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf8')
    (out, err) = p.communicate()
    ret = p.wait()
    if ret != 0:
        raise Exception(f'Could not execute {cmd} (exit code {ret}): {err}\nConsider installing with npm i @mapbox/togeojson')
    print(f"Created {len(out)} bytes of geojson")
    gdf = gpd.read_file(io.StringIO(out), driver='GeoJSON').fillna('')
    print(f"Read {len(gdf)} features from {kml_pathname}")
    return gdf
