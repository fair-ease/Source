# -*- coding: utf-8 -*-
import sys
import os
import numpy as np
import pandas as pd
import netCDF4
import time
import calendar
from SOURCE import find_variable_name

# Global variables
sleep_time = 0.1  # seconds
out_fill_value = 1.e20
out_time_reference = '1970-01-01T00:00:00Z'
out_reference_data = abs(calendar.timegm(time.strptime(out_time_reference, '%Y-%m-%dT%H:%M:%SZ')))


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Function to calculate distance from two points in spherical coordinates
def earth_distance(lat1, lon1, lat2, lon2):
    earth_radius = 6.371e3  # Earth radius (km)
    deg_to_rad = np.pi / 180.0  # Sexagesimal - radiants conversion factor
    # sexagesimal - radiants conversion
    rdlat1 = lat1 * deg_to_rad
    rdlon1 = lon1 * deg_to_rad
    rdlat2 = lat2 * deg_to_rad
    rdlon2 = lon2 * deg_to_rad
    # Distance calculus.
    # given A(lat1, lon1) e  B(lat2, lon2) on the unit sphere we have:
    # d(A, B) = arccos( cos(lon1 - lon2) * cos(lat1) * cos(lat2) + sin(lat1) * sin(lat2) )
    earth_dist = earth_radius * np.arccos(np.cos(rdlon1 - rdlon2) * np.cos(rdlat1) * np.cos(rdlat2)
                                          + np.sin(rdlat1) * np.sin(rdlat2))
    return earth_dist


# Functional version
def model_datasets_concatenator(in_list=None, in_variable_standard_name=None, in_csv_file=None, out_dir=None,
                                grid_observation_distance=None, out_variable_standard_name=None, mesh_mask_file=None,
                                first_date_str=None, last_date_str=None, verbose=True):
    if __name__ == '__main__':
        return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Model data nearest point extractor and concatenator.')
        print(' -------------------------')
    if in_list is None or in_variable_standard_name is None or in_csv_file is None or out_dir is None or \
            grid_observation_distance is None:
        time.sleep(sleep_time)
        print(' ERROR: 5 of 10 maximum arguments (5 optionals) not provided.', file=sys.stderr)
        print(' 1) Input datasets list;', file=sys.stderr)
        print(' 2) input variable standard_name to be concatenated;', file=sys.stderr)
        print(' 3) CSV location table with columns:', file=sys.stderr)
        print('     a) Output concatenated dataset file basename.', file=sys.stderr)
        print('     b) Longitude of location;', file=sys.stderr)
        print('     c) Latitude of location;', file=sys.stderr)
        print(' 4) Output directory;', file=sys.stderr)
        print(' 5) Grid-to-observation maximum acceptable distance (km)', file=sys.stderr)
        print(' 6) (optional) Output variable standard_name (for renaming) (default: no renaming);', file=sys.stderr)
        print(' 7) (optional) Mesh mask file (default: None);', file=sys.stderr)
        print(' 8) (optional) First date to evaluate in YYYYMMDD format'
              ' (default: first recorded date for each probe);', file=sys.stderr)
        print(' 9) (optional) Last date to evaluate in YYYYMMDD format'
              ' (default: last recorded date for each probe);', file=sys.stderr)
        print(' 10) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    try:
        time.strptime(first_date_str, '%Y%m%d')
    except (IndexError, TypeError, ValueError):
        try:
            time.strptime(first_date_str, '%Y-%m-%d %H:%M:%S')
        except (IndexError, TypeError, ValueError):
            first_date_str = None

    try:
        time.strptime(last_date_str, '%Y%m%d')
    except (IndexError, TypeError, ValueError):
        try:
            time.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')
        except (IndexError, TypeError, ValueError):
            last_date_str = None

    file_list = [element for element in in_list if element.endswith('.nc')]
    if not file_list:
        time.sleep(sleep_time)
        print(' Error. No processable files in input directory.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    if verbose:
        if not [element for element in in_list if element.endswith('.nc')]:
            print(' Input directory = ' + in_list)
        print(' Input variable standard_name: ' + in_variable_standard_name)
        print(' Input CSV location and out names table = ' + in_csv_file)
        print(' Output concatenated datasets directory = ' + out_dir)
        print(' Grid-to-observation maximum distance (km) = ' + str(grid_observation_distance))
        print(' Output variable standard_name (for renaming): '
              + str(out_variable_standard_name) + ' (if None the output variable name will not be renamed)')
        print(' Mesh mask file name = ' + str(mesh_mask_file) +
              ' (if None land overlap will be skipped:'
              ' remember to provide an already land overlapped variable in this case)')
        print(' First date to process = ' + str(first_date_str) +
              ' (if None it will be the first available date)')
        print(' Last date to process = ' + str(last_date_str) +
              ' (if None it will be the last available date)')
        print(' verbosity switch = ' + str(verbose))
        print(' -------------------------')
        print(' Starting process...')
        print(' -------------------------')

        in_list.sort()

        if (mesh_mask_file == 'None') or (mesh_mask_file == ''):
            mesh_mask_file = None

        if (out_variable_standard_name == 'None') or (out_variable_standard_name == ''):
            out_variable_standard_name = None
        if out_variable_standard_name is None:
            out_variable_standard_name = in_variable_standard_name

        if first_date_str is not None:
            try:
                first_date = time.strptime(first_date_str, '%Y%m%d')
            except ValueError:
                first_date = time.strptime(first_date_str, '%Y-%m-%d %H:%M:%S')

        if last_date_str is not None:
            try:
                last_date = time.strptime(last_date_str, '%Y%m%d')
            except ValueError:
                last_date = time.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')

        if (first_date_str is not None) and (last_date_str is not None):
            if first_date > last_date:
                time.sleep(sleep_time)
                print(' Error: selected first date is greater than last date. Exiting.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return

    if verbose:
        print(' Loading locations CSV file...')
    in_csv_data = open(in_csv_file, 'rb')
    in_csv_data = pd.read_csv(in_csv_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if in_csv_data.ndim == 1:
        in_csv_data = in_csv_data[np.newaxis, :]
    if in_csv_data.shape[0] == 0:
        time.sleep(sleep_time)
        print(' Warning. Empty locations CSV file for this variable. Exiting.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    if in_csv_data.ndim == 1:
        in_csv_data = in_csv_data.reshape(1, in_csv_data.shape[0])

    if mesh_mask_file is not None:
        if verbose:
            print(' Loading mesh mask file...')
        mesh_mask_data = netCDF4.Dataset(mesh_mask_file, mode='r')

    probes_platform_codes = in_csv_data[:, 0]
    locations_longitudes = in_csv_data[:, 1]
    locations_latitudes = in_csv_data[:, 2]

    if not os.path.exists(out_dir):
        if verbose:
            print(' Creating ' + out_dir + ' folder...')
        os.makedirs(out_dir)
    out_data = {}
    out_time = {}
    aggregated_time_data = np.empty(shape=0, dtype=np.float64)
    out_time_bounds = {}
    minimum_index = {}
    out_depth = {}
    out_variable = {}
    aggregated_variable_data = {}
    probes_indices = np.arange(probes_platform_codes.shape[0], dtype=int)
    rejected_indices = np.empty(shape=0, dtype=int)
    saved_record_type = None
    for model_dataset in in_list:
        try:
            in_variable_name = find_variable_name.find_variable_name(model_dataset, 'standard_name',
                                                                     in_variable_standard_name, verbose=False)
        except KeyError:
            in_variable_name = in_variable_standard_name
        model_data = netCDF4.Dataset(model_dataset, mode='r')
        try:
            model_time = model_data.variables['time']
        except KeyError:
            try:
                model_time = model_data.variables['time_counter']
            except KeyError:
                time.sleep(sleep_time)
                print(' Error: missing record variable in dataset ' + model_dataset + '.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                model_data.close()
                return
        model_time_reference = model_time.units
        if 'days' in model_time_reference:
            model_time_data = np.round(model_time[...] * 86400.)
        elif 'seconds' in model_time_reference:
            model_time_data = np.round(model_time[...])
        model_time_reference = model_time_reference[model_time_reference.find('since ') + len('since '):]
        try:
            model_reference_data = calendar.timegm(time.strptime(model_time_reference, '%Y-%m-%dT%H:%M:%SZ'))
        except ValueError:
            model_reference_data = calendar.timegm(time.strptime(model_time_reference, '%Y-%m-%d %H:%M:%S'))
        model_time_data = np.round((model_time_data + model_reference_data) / 600.) * 600
        if first_date_str is not None:
            if time.gmtime(model_time_data[-1]) < first_date:
                model_data.close()
                continue
        if last_date_str is not None:
            if time.gmtime(model_time_data[0]) > last_date:
                model_data.close()
                continue
        if len(model_time_data[model_time_data == 0.]) > 1:
            time.sleep(sleep_time)
            print(' Error: Zero valued time in dataset ' + model_dataset, file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            model_data.close()
            return
        if verbose:
            print(' Opening dataset ' + model_dataset)
        if model_time_data.shape[0] == 1:
            model_record_type = 'dm'
        elif model_time_data.shape[0] == 24:
            model_record_type = 'hm'
        if (saved_record_type is not None) and (model_record_type != saved_record_type):
            time.sleep(sleep_time)
            print(' Error: mixed daily and / or hourly mean model datasets in input directories.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            model_data.close()
            return
        saved_record_type = model_record_type
        try:
            model_longitude = model_data.variables['lon']
        except KeyError:
            try:
                model_longitude = model_data.variables['nav_lon']
            except KeyError:
                time.sleep(sleep_time)
                print(' Error: missing longitude variable.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                model_data.close()
                return
        try:
            model_latitude = model_data.variables['lat']
        except KeyError:
            try:
                model_latitude = model_data.variables['nav_lat']
            except KeyError:
                time.sleep(sleep_time)
                print(' Error: missing latitude variable.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                model_data.close()
                return
        if (model_latitude.ndim == 1) and (model_latitude.ndim == 1):
            model_longitude_data, model_latitude_data = np.meshgrid(model_longitude[...],
                                                                    model_latitude[...])
        elif (model_latitude.ndim == 2) and (model_latitude.ndim == 2):
            model_longitude_data = model_longitude[...]
            model_latitude_data = model_latitude[...]
        try:
            model_depth = model_data.variables['depth']
            in_mask_name = 'tmask'
        except KeyError:
            try:
                model_depth = model_data.variables['deptht']
                in_mask_name = 'tmask'
            except KeyError:
                try:
                    model_depth = model_data.variables['depthu']
                    in_mask_name = 'umask'
                except KeyError:
                    try:
                        model_depth = model_data.variables['depthv']
                        in_mask_name = 'vmask'
                    except KeyError:
                        try:
                            model_depth = model_data.variables['depthw']
                            in_mask_name = 'tmask'
                        except KeyError:
                            in_mask_name = 'tmask'

        if mesh_mask_file is not None:
            mask_variable = mesh_mask_data.variables[in_mask_name]
            mask_variable_data = mask_variable[...]
        try:
            model_depth_data = model_depth[...]
        except (IndexError, NameError, RuntimeError, ValueError):
            model_depth_data = np.array([0.])
        try:
            model_variable = model_data.variables[in_variable_name]
        except KeyError:
            time.sleep(sleep_time)
            print(' Warning: no fields with standard_name ' + in_variable_standard_name + ' in this file.',
                  file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            model_data.close()
            print(' Closing dataset ' + model_dataset)
            continue
        aggregated_time_data = np.append(aggregated_time_data, model_time_data)
        model_variable_data = model_variable[...]
        # Wave Peak Frequency - Wave Peak Period conversion
        if in_variable_name == 'fp':
            model_variable_data = 1 / model_variable_data
        if (mesh_mask_file is None) and (not np.ma.is_masked(model_variable_data)):
            time.sleep(sleep_time)
            print(' Warning: not masked input data, please apply mask file first.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            model_data.close()
            print(' Closing dataset ' + model_dataset)
            continue
        if mesh_mask_file:
            if model_variable.ndim == 3:
                if model_variable_data.shape[1:] != mask_variable_data.shape[2:]:
                    time.sleep(sleep_time)
                    print(' Error: model data and mask dimension mismatch. Exiting.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(' -------------------------')
                    return
            if model_variable.ndim == 4:
                if model_variable_data.shape[1:] != mask_variable_data.shape[1:]:
                    time.sleep(sleep_time)
                    print(' Error: model data and mask dimension mismatch. Exiting.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(' -------------------------')
                    return
        if probes_indices.shape[0] == 0:
            time.sleep(sleep_time)
            print(' Warning: no data do process. Exiting.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            model_data.close()
            return
        for location in range(probes_indices.shape[0]):
            probe_index = probes_indices[location]
            if probe_index in rejected_indices:
                continue
            probe_dictionary_index = str(probe_index)
            probe_platform_code = probes_platform_codes[location]
            out_file_name = 'model-data_' + probe_platform_code + \
                '_' + out_variable_standard_name + '_' + model_record_type + '_location-ported.nc'
            location_longitude = np.float32(locations_longitudes[location])
            location_latitude = np.float32(locations_latitudes[location])
            if probe_dictionary_index not in out_data.keys():
                if verbose:
                    print(' -------------------------')
                    print(' Analyzing location ' + str(location_longitude) + ', ' + str(location_latitude) + '...')
                    print(' Checking if probe distance from not filled model data is under '
                          + str(grid_observation_distance) + 'km...')
                model_distances = earth_distance(location_latitude * np.ones(shape=model_latitude_data.shape),
                                                 location_longitude * np.ones(shape=model_longitude_data.shape),
                                                 model_latitude_data, model_longitude_data)
                if mesh_mask_file is None:
                    if model_variable.ndim == 3:
                        surface_first_timestep_data = model_variable_data[0, ...]
                    elif model_variable.ndim == 4:
                        surface_first_timestep_data = model_variable_data[0, 0, ...]
                else:
                    if model_variable.ndim == 3:
                        surface_first_timestep_data = np.ma.masked_where(mask_variable_data[0, 0, ...] == 0,
                                                                         model_variable_data[0, ...])
                    elif model_variable.ndim == 4:
                        surface_first_timestep_data = np.ma.masked_where(mask_variable_data[0, 0, ...] == 0,
                                                                         model_variable_data[0, 0, ...])
                inverted_mask = np.invert(surface_first_timestep_data.mask)
                minimum_surface_distance = np.nanmin(model_distances[inverted_mask])
                if (minimum_surface_distance > np.float32(grid_observation_distance)) \
                        or (np.isnan(minimum_surface_distance)):
                    high_distance_points_switch = True
                    if verbose:
                        print(' NO (' + str(np.round(minimum_surface_distance, decimals=2)) + ' km).')
                else:
                    high_distance_points_switch = False
                    if verbose:
                        print(' OK (' + str(np.round(minimum_surface_distance, decimals=2)) + ' km).')
                if high_distance_points_switch:
                    if verbose:
                        time.sleep(sleep_time)
                        print(' Warning: location ' + str(location_longitude) + ', ' + str(location_latitude) +
                              ' has been rejected because of high horizontal distance'
                              ' from first point valid model data.', file=sys.stderr)
                        time.sleep(sleep_time)
                        print(' -------------------------')
                    rejected_indices = np.append(rejected_indices, probe_index)
                    continue
                minimum_index[probe_dictionary_index] = np.where(model_distances == minimum_surface_distance)
                if verbose:
                    print(' Creating output dataset: ' + out_file_name)
                # Create output dataset
                out_data[probe_dictionary_index] = netCDF4.Dataset(out_dir + out_file_name,
                                                                   mode='w', format='NETCDF4')
                if verbose:
                    print(' Creating location ' + str(location_longitude) + ', '
                          + str(location_latitude) + ' dimensions.')
                if model_variable_data.ndim == 3:
                    out_data[probe_dictionary_index].createDimension('depth', 1)
                elif model_variable_data.ndim == 4:
                    out_data[probe_dictionary_index].createDimension('depth', model_depth_data.shape[0])
                out_data[probe_dictionary_index].createDimension('time', None)
                out_data[probe_dictionary_index].createDimension('axis_nbounds', 2)
                try:
                    out_data[probe_dictionary_index].bulletin_type = model_data.bulletin_type
                except AttributeError:
                    pass
                try:
                    out_data[probe_dictionary_index].institution = model_data.institution
                except AttributeError:
                    pass
                try:
                    out_data[probe_dictionary_index].source = model_data.source
                except AttributeError:
                    pass
                if verbose:
                    print(' Creating location ' + str(location_longitude) + ', ' + str(location_latitude) +
                          ' output dimension variables.')
                model_longitude_variable = out_data[probe_dictionary_index].createVariable('lon', datatype=np.float32)
                model_longitude_variable[...] = model_longitude_data[minimum_index[probe_dictionary_index]]
                model_longitude_variable.long_name = 'Model Location Nearest Longitude from ' + str(location_longitude)
                model_longitude_variable.standard_name = 'longitude'
                model_longitude_variable.units = 'degrees_east'
                model_longitude_variable.axis = 'X'
                model_latitude_variable = out_data[probe_dictionary_index].createVariable('lat', datatype=np.float32)
                model_latitude_variable[...] = model_latitude_data[minimum_index[probe_dictionary_index]]
                model_latitude_variable.long_name = 'Model Location Nearest Latitude from ' + str(location_latitude)
                model_latitude_variable.standard_name = 'latitude'
                model_latitude_variable.units = 'degrees_north'
                model_latitude_variable.axis = 'Y'
                location_model_distance_variable =\
                    out_data[probe_dictionary_index].createVariable('location_model_distance', datatype=np.float32)
                location_model_distance_variable[...] = minimum_surface_distance
                location_model_distance_variable.long_name = 'Location - Model Horizontal Distance'
                location_model_distance_variable.standard_name = 'location_model_distance'
                location_model_distance_variable.units = 'km'
                out_depth[probe_dictionary_index] =\
                    out_data[probe_dictionary_index].createVariable('depth', datatype=np.float32,
                                                                    dimensions=('depth',))
                if model_variable_data.ndim == 3:
                    out_depth[probe_dictionary_index][...] = np.array([0.])
                elif model_variable_data.ndim == 4:
                    out_depth[probe_dictionary_index][...] = model_depth_data
                out_depth[probe_dictionary_index].positive = 'down'
                out_depth[probe_dictionary_index].long_name = 'Model depth'
                out_depth[probe_dictionary_index].standard_name = 'depth'
                out_depth[probe_dictionary_index].units = 'm'
                out_depth[probe_dictionary_index].axis = 'Z'
                if out_depth[probe_dictionary_index][...].shape[0] > 1:
                    out_depth[probe_dictionary_index].valid_min = np.min(out_depth[probe_dictionary_index][...])
                    out_depth[probe_dictionary_index].valid_max = np.max(out_depth[probe_dictionary_index][...])
                out_time[probe_dictionary_index] = \
                    out_data[probe_dictionary_index].createVariable('time', datatype=np.float64, dimensions=('time',),
                                                                    zlib=True, complevel=1)
                out_time[probe_dictionary_index].bounds = 'time_bounds'
                out_time[probe_dictionary_index].long_name = 'Time'
                out_time[probe_dictionary_index].standard_name = 'time'
                out_time[probe_dictionary_index].units = 'seconds since ' + out_time_reference
                out_time[probe_dictionary_index].calendar = 'gregorian'
                out_time[probe_dictionary_index].axis = 'T'
                out_time_bounds[probe_dictionary_index] = \
                    out_data[probe_dictionary_index].createVariable('time_bounds', datatype=np.float64,
                                                                    dimensions=('time', 'axis_nbounds'),
                                                                    zlib=True, complevel=1)
                out_time_bounds[probe_dictionary_index].units = 'seconds since ' + out_time_reference
                out_variable[probe_dictionary_index] = \
                    out_data[probe_dictionary_index].createVariable(out_variable_standard_name, datatype=np.float32,
                                                                    dimensions=('time', 'depth'),
                                                                    fill_value=out_fill_value, zlib=True, complevel=1)
                out_variable[probe_dictionary_index].missing_value = np.float32(out_fill_value)
                out_variable[probe_dictionary_index].standard_name = out_variable_standard_name
                try:
                    out_variable[probe_dictionary_index].long_name = model_variable.long_name
                except AttributeError:
                    pass
                try:
                    out_variable[probe_dictionary_index].units = model_variable.units
                except AttributeError:
                    pass
                aggregated_variable_data[probe_dictionary_index] =\
                    np.ma.empty(shape=(0, out_depth[probe_dictionary_index][...].shape[0]), fill_value=out_fill_value,
                                dtype=model_variable.datatype)
                if verbose:
                    print(' -------------------------')

            if verbose:
                print(' Extracting and writing data to ' + out_file_name +
                      ' ' + out_variable_standard_name + ' output variable..')

            if model_variable_data.ndim == 3:
                model_variable_location_cut = model_variable_data[..., minimum_index[probe_dictionary_index][0],
                                                                  minimum_index[probe_dictionary_index][1]]
                if mesh_mask_file is not None:
                    mask_variable_location_cut = mask_variable_data[..., 0, minimum_index[probe_dictionary_index][0],
                                                                    minimum_index[probe_dictionary_index][1]]
                    if model_variable_location_cut.shape[0] > 1:
                        mask_variable_location_cut = np.repeat(mask_variable_location_cut,
                                                               model_variable_location_cut.shape[0], axis=0)
                    model_variable_location_cut = np.ma.masked_where(mask_variable_location_cut == 0,
                                                                     model_variable_location_cut)
            elif model_variable_data.ndim == 4:
                model_variable_location_cut = model_variable_data[..., minimum_index[probe_dictionary_index][0],
                                                                  minimum_index[probe_dictionary_index][1]][..., 0]
                if mesh_mask_file is not None:
                    mask_variable_location_cut = mask_variable_data[..., minimum_index[probe_dictionary_index][0],
                                                                    minimum_index[probe_dictionary_index][1]][..., 0]
                    if model_variable_location_cut.shape[0] > 1:
                        mask_variable_location_cut = np.repeat(mask_variable_location_cut,
                                                               model_variable_location_cut.shape[0], axis=0)
                    model_variable_location_cut = np.ma.masked_where(mask_variable_location_cut == 0,
                                                                     model_variable_location_cut)
            if not high_distance_points_switch:
                aggregated_variable_data[probe_dictionary_index] =\
                    np.ma.append(aggregated_variable_data[probe_dictionary_index], model_variable_location_cut, axis=0)
            else:
                aggregated_variable_data[probe_dictionary_index] =\
                    np.ma.append(aggregated_variable_data[probe_dictionary_index],
                                 np.ma.masked_all(shape=model_variable_location_cut.shape), axis=0)

        if verbose:
            print(' Closing dataset ' + model_dataset)
        model_data.close()

    if aggregated_time_data.shape[0] == 0:
        time.sleep(sleep_time)
        print(' Warning. No data for ' + in_variable_standard_name + ' field.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    [unique_time_data, unique_counts] = np.unique(aggregated_time_data, return_counts=True)
    duplicated_time_data = unique_time_data[unique_counts > 1]
    duplicated_counts = unique_counts[unique_counts > 1]
    if len(duplicated_time_data) > 0:
        unique_time_stamps = pd.to_datetime(duplicated_time_data * 1.e9)
        time.sleep(sleep_time)
        print(' Error. There is duplication in aggregated time data.', file=sys.stderr)
        print(' Duplicated time stamps:', file=sys.stderr)
        for duplicated_index in range(len(duplicated_time_data)):
            print(' ' + str(unique_time_stamps[duplicated_index]) +
                  ',  count = ' + str(duplicated_counts[duplicated_index]), file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    sort_indices = np.argsort(aggregated_time_data)
    aggregated_time_data = aggregated_time_data[sort_indices]
    start_date = pd.to_datetime(np.min(aggregated_time_data) * 1.e9)
    end_date = pd.to_datetime(np.max(aggregated_time_data) * 1.e9)
    if model_record_type == 'dm':
        out_time_stamps = pd.date_range(start_date, end_date, freq='D')
        out_left_time_stamps = out_time_stamps - pd.Timedelta(hours=12)
        out_right_time_stamps = out_time_stamps + pd.Timedelta(hours=12)
    if model_record_type == 'hm':
        out_time_stamps = pd.date_range(start_date, end_date, freq='H')
        out_left_time_stamps = out_time_stamps - pd.Timedelta(minutes=30)
        out_right_time_stamps = out_time_stamps + pd.Timedelta(minutes=30)
    out_time_data = np.array([t.value / 1e9 for t in out_time_stamps])
    out_time_bounds_data = np.transpose(np.array(
        [[t.value / 1e9 for t in out_left_time_stamps], [t.value / 1e9 for t in out_right_time_stamps]]))
    out_index_time_data = \
        [index for index in range(out_time_data.shape[0]) if out_time_data[index] in aggregated_time_data]

    for location in range(probes_indices.shape[0]):
        probe_index = probes_indices[location]
        if probe_index in rejected_indices:
            continue
        probe_dictionary_index = str(probe_index)
        probe_platform_code = probes_platform_codes[location]
        out_file_name = 'model-data_' + probe_platform_code + \
            '_' + out_variable_standard_name + '_' + model_record_type + '_location-ported.nc'
        location_longitude = np.float32(locations_longitudes[location])
        location_latitude = np.float32(locations_latitudes[location])
        if verbose:
            print(' -------------------------')
            print(' Writing location ' + str(location_longitude) + ', ' + str(location_latitude) +
                  ' output time and ' + out_variable_standard_name + ' variables...')
        out_time[probe_dictionary_index][...] = out_time_data + out_reference_data

        out_time_bounds[probe_dictionary_index][...] = out_time_bounds_data + out_reference_data

        out_variable_data = np.ma.masked_all(shape=(out_time_data.shape[0],
                                                    out_depth[probe_dictionary_index][...].shape[0]),
                                             dtype=aggregated_variable_data[probe_dictionary_index].dtype)
        out_variable_data.mask[out_index_time_data, ...] = False
        out_variable_data[out_index_time_data, ...] = aggregated_variable_data[probe_dictionary_index][sort_indices]
        out_variable[probe_dictionary_index][...] = out_variable_data
        out_variable[probe_dictionary_index].valid_min = np.ma.min(out_variable[probe_dictionary_index][...])
        out_variable[probe_dictionary_index].valid_max = np.ma.max(out_variable[probe_dictionary_index][...])
        if verbose:
            print(' Setting ' + out_file_name + ' variables and global attributes...')
        out_data[probe_dictionary_index].institution = \
            'Istituto Nazionale di Geofisica e Vulcanologia - Bologna, Italy'
        out_data[probe_dictionary_index].source = 'SOURCE evaluation system'
        out_data[probe_dictionary_index].SOURCE_platform_code = probe_platform_code
        out_data[probe_dictionary_index].conventions = 'CF-1.6'
        out_data[probe_dictionary_index].field_type = 'Location ported model data'
        out_data[probe_dictionary_index].history = \
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + ' : Post processed and location ported\n'
        if model_record_type == 'hm':
            out_data[probe_dictionary_index].title = 'Hourly Mean Time Series'
        elif model_record_type == 'dm':
            out_data[probe_dictionary_index].title = 'Daily Mean Time Series'
        if verbose:
            print(' Closing output ' + out_file_name + ' dataset...')
        out_data[probe_dictionary_index].close()

    if mesh_mask_file is not None:
        if verbose:
            print(' -------------------------')
            print(' Closing mesh mask file...')
            print(' -------------------------')
        mesh_mask_data.close()


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_dir = sys.argv[1]
        in_variable_standard_name = sys.argv[2]
        in_csv_file = sys.argv[3]
        out_dir = sys.argv[4]
        grid_observation_distance = sys.argv[5]

    except (IndexError, ValueError):
        in_dir = None
        in_variable_standard_name = None
        in_csv_file = None
        out_dir = None
        grid_observation_distance = None

    try:
        out_variable_standard_name = sys.argv[6]
    except (IndexError, ValueError):
        out_variable_standard_name = None

    try:
        mesh_mask_file = sys.argv[7]
    except (IndexError, ValueError):
        mesh_mask_file = None

    try:
        first_date_str = sys.argv[8]
    except (IndexError, ValueError):
        first_date_str = None

    try:
        last_date_str = sys.argv[9]
    except (IndexError, ValueError):
        last_date_str = None

    try:
        verbose = string_to_bool(sys.argv[10])
    except (IndexError, ValueError):
        verbose = True

    model_datasets_concatenator(in_dir, in_variable_standard_name, in_csv_file, out_dir,
                                grid_observation_distance, out_variable_standard_name, mesh_mask_file,
                                first_date_str, last_date_str, verbose)
