
def create_history_data (source,destination,flag):
  import shutil
  #print(source)
  #print(destination)
  #print(flag)
  if flag :
    print("Update mode")
  else:
    print("Creation mode")
    shutil.copytree(source,destination,dirs_exist_ok=True)


def sorted_list_symlink (InDir,WorkDir):
  import os
  sorted_lista = sorted([ x for x in os.listdir(InDir) if x.endswith('T.nc')])
#  print(sorted_lista)
  for ff in sorted_lista:
#    print(InDir+'/'+ff,WorkDir+'/'+ff)
    os.symlink(InDir+ff,WorkDir+ff)


def shell_create_map(output_dir,aoi_geom,flag_bbox,map):
  pass


def jupiter_create_map_aoi(aoi_geom,flag_bbox,map):
  import folium
  import geopandas as gpd
  aoi_poly_geom = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[aoi_geom]) 
  m = folium.Map(location=[map[0], map[1]], zoom_start=map[2])
  folium.GeoJson(aoi_geom,color='red').add_to(m)
  folium.LatLngPopup().add_to(m)
  return m


def jupiter_create_map_checkdata(aoi_geom,flag_bbox,map):
  import folium
  import geopandas as gpd
  m = folium.Map(location=[map[0], map[1]], zoom_start=map[2])
  return m



