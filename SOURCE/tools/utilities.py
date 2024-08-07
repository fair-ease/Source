
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


def jupiter_create_map_checkdata(aoi_geom,edge,flag_bbox,map,numberOfFiles,subset):
  import folium
  import geopandas as gpd
  import random
  def popup_data(files,i):
    from datetime import datetime
    date_time = datetime.fromisoformat(files.iloc[i]['last_date_observation'].replace('Z', '+00:00'))
    date = date_time.strftime('%Y %b %d')
    time = date_time.strftime('%H:%M:%S')
    html = str(
      '<b>Platform code: </b><nowrap>' + str(files.iloc[i]['platform_code']) + '<br>' +
      '<b>Institution: </b><nowrap>' + files.iloc[i]['institution'] + '<br>' +
      '<b>Last Lat.: </b><nowrap>' + str(files.iloc[i]['last_latitude_observation']) + '<br>' +
      '<b>Last Lon.: </b><nowrap>' + str(files.iloc[i]['last_longitude_observation']) + '<br>' +
      '<b>Last Observation.: </b><nowrap>' + date + ' - ' + time + '<br>'
    )
    return html

  m = folium.Map(location=[map[0], map[1]], zoom_start=map[2])
  folium.GeoJson(aoi_geom,color='red').add_to(m)
  folium.LatLngPopup().add_to(m)
  for platform, files in subset[:numberOfFiles].groupby(['platform_code', 'data_type']):
    i = len(files)-1
    popup = folium.Popup(popup_data(files,i), min_width=150, max_width=400)
    m.add_child(folium.Marker([files.iloc[i]['last_latitude_observation'], files.iloc[i]['last_longitude_observation']], popup = popup ))
  #Zooming closer
  #m.fit_bounds(bounds=edge, max_zoom=8)
  return m



