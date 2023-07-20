import ftputil
import os
import pandas as pd
import datetime
from collections import namedtuple

from shapely.geometry import box, Point

def getIndexFiles(usr,pas,dataset):
    #Provides the index files available at the ftp server
    indexes = dataset['index_files'] + [dataset['index_platform']]
    with ftputil.FTPHost(dataset['host'], usr, pas) as ftp_host:  # connect to CMEMS FTP
        for index in indexes:
            remotefile= "/".join(['Core',dataset['product'],dataset['name'],index])
            print('.....Downloading ' + index)
            localfile = os.path.join(os.getcwd(),'data','index_files',index)
            ftp_host.download(remotefile,localfile)  # remote, local
    print('Ready!')

def readIndexFileFromCWD(path2file, targeted_bbox):
    #Load as pandas dataframe the file in the provided path
    filename = os.path.basename(path2file)
    print('...Loading info from: '+filename)
    if targeted_bbox != None:
        raw_index_info =[]
        chunks = pd.read_csv(path2file, skiprows=5,chunksize=1000)
        for chunk in chunks:
            chunk['spatialOverlap'] = chunk.apply(spatialOverlap,targeted_bbox=targeted_bbox,axis=1)
            raw_index_info.append(chunk[chunk['spatialOverlap'] == True])
        return pd.concat(raw_index_info)
    else:
        result = pd.read_csv(path2file, skiprows=5)
        try:
            result = result.rename(columns={"provider_edmo_code": "institution_edmo_code"})
        except Exception as e:
            pass
        return result

def spatialOverlap(row, targeted_bbox):
    # Checks if a file contains data in the specified area (targeted_bbox)
    result = False
    try:
        geospatial_lat_min = float(row['geospatial_lat_min'])
        geospatial_lat_max = float(row['geospatial_lat_max'])
        geospatial_lon_min = float(row['geospatial_lon_min'])
        geospatial_lon_max = float(row['geospatial_lon_max'])
        targeted_bounding_box = box(targeted_bbox[0], targeted_bbox[1],targeted_bbox[2], targeted_bbox[3])
        bounding_box = box(geospatial_lon_min, geospatial_lat_min,geospatial_lon_max, geospatial_lat_max)
        if targeted_bounding_box.intersects(bounding_box):  # check other rules on https://shapely.readthedocs.io/en/stable/manual.html
            result = True
    except Eception as e:
        pass
    return result

def getIndexFilesInfo(usr, pas, dataset, targeted_bbox):
    # Load and merge in a single entity all the information contained on each file descriptor of a given dataset
    # 1) Loading the index platform info as dataframe
    path2file = os.path.join(os.path.join(os.path.join(os.getcwd(),'data'), 'index_files'), dataset['index_platform'])
    indexPlatform = readIndexFileFromCWD(path2file, None)
    indexPlatform.rename(columns={indexPlatform.columns[0]: "platform_code" }, inplace = True)
    indexPlatform = indexPlatform.drop_duplicates(subset='platform_code', keep="first")
    # 2) Loading the index files info as dataframes
    netcdf_collections = []
    for filename in dataset['index_files']:
        path2file = os.path.join(os.getcwd(),'data', 'index_files',filename)
        indexFile = readIndexFileFromCWD(path2file, targeted_bbox)
        netcdf_collections.append(indexFile)
    netcdf_collections = pd.concat(netcdf_collections)
    # 3) creating new columns: derived info
    netcdf_collections['netcdf'] = netcdf_collections['file_name'].str.split('/').str[-1]
    netcdf_collections['file_type'] = netcdf_collections['netcdf'].str.split('.').str[0].str.split('_').str[1]
    netcdf_collections['data_type'] = netcdf_collections['netcdf'].str.split('.').str[0].str.split('_').str[2]
    netcdf_collections['platform_code'] = netcdf_collections['netcdf'].str.split('.').str[0].str.split('_').str[3]
    # 4) Merging the information of all files
    headers = ['platform_code','wmo_platform_code', 'institution_edmo_code', 'last_latitude_observation', 'last_longitude_observation','last_date_observation']
    result = pd.merge(netcdf_collections,indexPlatform[headers],on='platform_code')
    print('Ready!')
    return result


def timeOverlap(row, targeted_range):
    # Checks if a file contains data in the specified time range (targeted_range)
    result = False
    try:
        date_format = "%Y-%m-%dT%H:%M:%SZ"
        targeted_ini = datetime.datetime.strptime(targeted_range.split('/')[0], date_format)
        targeted_end = datetime.datetime.strptime(targeted_range.split('/')[1], date_format)
        time_start = datetime.datetime.strptime(row['time_coverage_start'],date_format)
        time_end = datetime.datetime.strptime(row['time_coverage_end'],date_format)
        Range = namedtuple('Range', ['start', 'end'])
        r1 = Range(start=targeted_ini, end=targeted_end)
        r2 = Range(start=time_start, end=time_end)
        latest_start = max(r1.start, r2.start)
        earliest_end = min(r1.end, r2.end)
        delta = (earliest_end - latest_start).days + 1
        overlap = max(0, delta)
        if overlap != 0:
            result = True
    except Exception as e:
        pass
    return result
