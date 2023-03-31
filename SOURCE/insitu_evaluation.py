# -*- coding: utf-8 -*-
import sys
import os
import shlex
import time
import calendar
import numpy as np
import pandas as pd
import netCDF4
from SOURCE import find_variable_name, time_calc

# Global variables
sleep_time = 0.1  # seconds
out_fill_value = 1.e20
out_time_reference = '1970-01-01T00:00:00Z'
out_reference_data = abs(calendar.timegm(time.strptime(out_time_reference, '%Y-%m-%dT%H:%M:%SZ')))


# Function to pass hh:mm:ss to seconds
def to_seconds(time_string):
    seconds = 0
    for part in time_string.split(':'):
        seconds = seconds * 60 + int(part)
    return seconds


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
def insitu_evaluation(in_csv_dir=None, first_dir=None, second_dir=None, in_fields_standard_name_str=None, out_dir=None,
                      first_date_str=None, last_date_str=None, first_title_str=None, second_title_str=None):
    """
    Script to compute two different databases evaluation (insitu - insitu, insitu - model, etc.).

    Input arguments:

        1) In situ information CSV directory: Directory with (almost) the sequent files):
            a) Devices information CSV file, with the sequent header:
                Device ID, Name;
            b) Organizations information CSV file, with the sequent header:
                Organization ID, Name, Country, Link;
            c) Variables information CSV file, with the sequent header:
                Variable ID, long_name, standard_name, units;
            d) Probes information CSV file, with the sequent header:
                Probe ID, platform_code, name, WMO, device type ID, organization ID, variable IDs,
                average longitudes, average latitudes, record starts, record ends, sampling times (ddd hh:mm:ss form),
                depth levels, quality controls, ancillary notes, weblink;

        2) First directory with post processed files in netCDF format;

        3) Second directory with post processed files in netCDF format;

        4) Input variables standard_name attributes to process space separated string
            (for example: "sea_water_temperature sea_water_practical_salinity", please read CF conventions standard name
            table to find the correct strings to insert);

        5) Output evaluation datasets directory;

        6) First date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format (OPTIONAL);

        7) Last date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS (OPTIONAL);

        8) First devices comprehension string (OPTIONAL);

        9) Second devices comprehension string (OPTIONAL).

    Output files are in netCDF-4 format, divided by parameter and probe platform_code, containing:

        1) first horizontal coordinates;
        2) second horizontal coordinates;
        3) field depths, if field is 4D;
        4) time counter and boundaries;
        5) first time series;
        6) second time series;
        7) RMSE profile time series;
        8) BIAS profile time series;
        8) time averaged RMSE profile;
        9) time averaged BIAS profile;
        11) global attributes containing additional information.

    Written Nov 20, 2017 by Paolo Oliveri
    """
    if __name__ == '__main__':
        return
    start_run_time = calendar.timegm(time.gmtime())
    print('-------------------------' + ' ' + __file__ + ' -------------------------')
    print(' Local time is ' + time.strftime("%a %b %d %H:%M:%S %Z %Y", time.gmtime()))
    print(' -------------------------')
    print(' Script to compute two different insitu datasets evaluation.')
    print(' -------------------------')
    if in_csv_dir is None or first_dir is None or second_dir is None or in_fields_standard_name_str is None or \
            out_dir is None:
        time.sleep(sleep_time)
        print(' ERROR: 5 of 9 maximum arguments (4 optionals) not provided.', file=sys.stderr)
        print(' 1) In situ information CSV directory;', file=sys.stderr)
        print(' 2) First netCDF data directory;', file=sys.stderr)
        print(' 3) Second netCDF data directory;', file=sys.stderr)
        print(' 4) Input fields standard_name space separated string to process'
              ' (for example: "sea_water_temperature sea_water_practical_salinity");', file=sys.stderr)
        print(' 5) Output directory;', file=sys.stderr)
        print(' 6) (optional) First date to evaluate in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: first recorded date for each device);', file=sys.stderr)
        print(' 7) (optional) Last date to evaluate in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: last recorded date for each device);', file=sys.stderr)
        print(' 8) (optional) First devices comprehension variable name title (with no spaces)'
              ' (default: "first");', file=sys.stderr)
        print(' 9) (optional) Second devices comprehension variable name title (with no spaces)'
              ' (default: "second").', file=sys.stderr)
        time.sleep(sleep_time)
        return

    try:
        first_date = time.strptime(first_date_str, '%Y%m%d')
    except (IndexError, TypeError, ValueError):
        try:
            first_date = time.strptime(first_date_str, '%Y-%m-%d %H:%M:%S')
        except (IndexError, TypeError, ValueError):
            first_date_str = None
            first_date = None

    try:
        last_date = time.strptime(last_date_str, '%Y%m%d')
    except (IndexError, TypeError, ValueError):
        try:
            last_date = time.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')
        except (IndexError, TypeError, ValueError):
            last_date_str = None
            last_date = None

    if (first_title_str is None) or (first_title_str == 'None') or (first_title_str == ''):
        first_title_str = 'first'
    if (second_title_str is None) or (second_title_str == 'None') or (second_title_str == ''):
        second_title_str = 'second'

    print(' in situ information CSV directory = ' + in_csv_dir)
    print(' Input first netCDF post processed data directory = ' + first_dir)
    print(' Input second netCDF post processed data directory = ' + second_dir)
    print(' Input variables to process standard_name string = ' + in_fields_standard_name_str)
    print(' Output directory = ' + out_dir)
    print(' First date to process = ' + str(first_date_str) +
          ' (if None it will be the first available date for each device)')
    print(' Last date to process = ' + str(last_date_str) +
          ' (if None it will be the last available date for each device)')
    print(' First devices comprehension variable name title = ' + str(first_title_str))
    print(' Second devices comprehension variable name title = ' + str(second_title_str))
    print(' -------------------------')

    if (in_fields_standard_name_str is None) or (in_fields_standard_name_str == 'None') or \
            (in_fields_standard_name_str == '') or len(in_fields_standard_name_str.split(' ')) < 1:
        time.sleep(sleep_time)
        print(' Error. Wrong input fields string.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    else:
        in_fields_standard_name_list = shlex.split(in_fields_standard_name_str)

    if not os.path.exists(out_dir):
        print(' Creating output directory.')
        print(' -------------------------')
        os.makedirs(out_dir)
    if not os.listdir(out_dir):
        pass
    else:
        time.sleep(sleep_time)
        print(' Warning: existing files or directories in output directory.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')

    print(' Loading devices information CSV file...')
    try:
        in_devices_data = open(in_csv_dir + '/devices.csv', 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty devices CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    in_devices_data =\
        pd.read_csv(in_devices_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if in_devices_data.ndim == 1:
        in_devices_data = in_devices_data[np.newaxis, :]

    device_ids = in_devices_data[:, 0]
    device_types = in_devices_data[:, 1]

    print(' Loading organizations information CSV file...')
    try:
        in_organizations_data = open(in_csv_dir + '/organizations.csv', 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty organizations CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    in_organizations_data =\
        pd.read_csv(in_organizations_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if in_organizations_data.ndim == 1:
        in_organizations_data = in_organizations_data[np.newaxis, :]

    organization_ids = in_organizations_data[:, 0]
    organization_names = in_organizations_data[:, 1]

    print(' Loading variables information CSV file...')
    try:
        in_variables_data = open(in_csv_dir + '/variables.csv', 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty variables CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    in_variables_data =\
        pd.read_csv(in_variables_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if in_variables_data.ndim == 1:
        in_variables_data = in_variables_data[np.newaxis, :]

    variable_ids = in_variables_data[:, 0]
    variable_standard_names = in_variables_data[:, 1]

    print(' Loading probes information CSV file...')
    try:
        in_probes_data = open(in_csv_dir + '/probes.csv', 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty probes CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    in_probes_data =\
        pd.read_csv(in_probes_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if in_probes_data.ndim == 1:
        in_probes_data = in_probes_data[np.newaxis, :]

    probes_platform_codes = in_probes_data[:, 1]
    probes_names = in_probes_data[:, 2]
    probes_wmo = in_probes_data[:, 3]
    probes_device_type_ids = in_probes_data[:, 4]
    probes_types = np.copy(probes_device_type_ids)
    for index in range(len(device_types)):
        probes_types = np.where(probes_types == device_ids[index], device_types[index], probes_types)
    probes_organization_ids = in_probes_data[:, 5]
    probes_organizations = np.copy(probes_organization_ids)
    for index in range(len(organization_names)):
        probes_organizations =\
            np.where(probes_organizations == organization_ids[index], organization_names[index], probes_organizations)
    probes_variables_ids = [standard_names.split(';') for standard_names in in_probes_data[:, 6]]
    probes_standard_names = probes_variables_ids
    for index in range(len(probes_variables_ids)):
        for index_id in range(len(variable_ids)):
            try:
                probe_variable_index = probes_standard_names[index].index(variable_ids[index_id])
                probes_standard_names[index][probe_variable_index] = variable_standard_names[index_id]
            except ValueError:
                continue
    probes_sampling_times = [sampling_times.split(';') for sampling_times in in_probes_data[:, 11]]

    if first_date_str is not None:
        first_date_seconds = calendar.timegm(first_date)
    if last_date_str is not None:
        last_date_seconds = calendar.timegm(last_date)

    print(' -------------------------')
    for insitu_device in range(probes_platform_codes.shape[0]):
        device_name = probes_names[insitu_device]
        device_platform_code = probes_platform_codes[insitu_device]
        # if device_platform_code != '':
        #     continue
        device_sampling_times = probes_sampling_times[insitu_device]
        print(' Evaluating device ' + device_platform_code + ' (' + device_name + ') ...')
        device_standard_names = np.array(probes_standard_names[insitu_device])
        if ('sea_water_speed' in device_standard_names) and\
                ('direction_of_sea_water_velocity' in device_standard_names):
            if 'eastward_sea_water_velocity' not in device_standard_names:
                device_standard_names = np.append(device_standard_names, 'eastward_sea_water_velocity')
            if 'northward_sea_water_velocity' not in device_standard_names:
                device_standard_names = np.append(device_standard_names, 'northward_sea_water_velocity')
        if ('eastward_sea_water_velocity' in device_standard_names) and \
                ('northward_sea_water_velocity' in device_standard_names):
            if 'sea_water_speed' not in device_standard_names:
                device_standard_names = np.append(device_standard_names, 'sea_water_speed')
            if 'direction_of_sea_water_velocity' not in device_standard_names:
                device_standard_names = np.append(device_standard_names, 'direction_of_sea_water_velocity')

        if device_standard_names.shape[0] == 0:
            time.sleep(sleep_time)
            print(' Warning: no fields to process for this device.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            continue
        print(' Fields to process: ' + ' '.join(map(str, device_standard_names)))
        for variable_standard_name in device_standard_names:
            if variable_standard_name not in in_fields_standard_name_list:
                continue
            print(' -------------------------')
            first_field_dir = first_dir + '/' + variable_standard_name + '/'
            second_field_dir = second_dir + '/' + variable_standard_name + '/'
            variable_name_index = int(np.where(device_standard_names == variable_standard_name)[0])
            try:
                device_field_sampling_time = device_sampling_times[variable_name_index][0]
            except IndexError:
                variable_name_index = int(np.where(device_standard_names == 'sea_water_speed')[0])
                try:
                    device_field_sampling_time = device_sampling_times[variable_name_index][0]
                except IndexError:
                    variable_name_index = int(np.where(device_standard_names == 'eastward_sea_water_velocity')[0])
                    device_field_sampling_time = device_sampling_times[variable_name_index][0]
            print(' Comparing data field ' + variable_standard_name)
            try:
                first_file = [first_field_dir + file for file in os.listdir(first_field_dir) if
                              device_platform_code + '_' + variable_standard_name + '_' in file][0]
            except (OSError, IndexError):
                time.sleep(sleep_time)
                print(' Warning: device ' + device_platform_code + ' (' + device_name + ') '
                      ' dataset in first directory not found. Skipping.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue
            first_sampling_time_seconds = time_calc.time_calc(first_file, verbose=False)
            first_sampling_time_days = first_sampling_time_seconds // 86400
            first_sampling_time_modulus = first_sampling_time_seconds - first_sampling_time_days * 86400
            first_sampling_time = time.strftime('%H:%M:%S', time.gmtime(first_sampling_time_modulus))
            if first_sampling_time_seconds == 1:
                print(' most representative sampling time: 1 second')
            elif first_sampling_time_seconds < 60:
                print(' most representative sampling time: ' + first_sampling_time + ' seconds')
            elif first_sampling_time_seconds == 60:
                print(' most representative sampling time: ' + first_sampling_time + ' minute')
            elif first_sampling_time_seconds < 3600:
                print(' most representative sampling time: ' + first_sampling_time + ' minutes')
            elif first_sampling_time_seconds == 3600:
                print(' most representative sampling time: ' + first_sampling_time + ' hour')
            elif first_sampling_time_seconds < 86400:
                print(' most representative sampling time: ' + first_sampling_time + ' hours')
            elif first_sampling_time_seconds == 86400:
                print(' most representative sampling time: 1 day')
            first_data = netCDF4.Dataset(first_file, mode='r')
            print(' Loading ' + first_file + ' data file...')
            first_longitude = first_data.variables['lon']
            first_longitude_data = first_longitude[...]
            first_latitude = first_data.variables['lat']
            first_latitude_data = first_latitude[...]
            first_depth = first_data.variables['depth']
            first_depth_data = first_depth[...]
            first_time = first_data.variables['time']
            first_time_reference = first_time.units
            if 'days' in first_time_reference:
                first_time_data = np.round(first_time[...] * 86400.)
            elif 'seconds' in first_time_reference:
                first_time_data = np.round(first_time[...])
            first_time_reference = first_time_reference[first_time_reference.find('since ') + len('since '):]
            first_reference_data = abs(calendar.timegm(time.strptime(first_time_reference, '%Y-%m-%dT%H:%M:%SZ')))
            first_time_data += - first_reference_data
            if (first_date_str is not None) or (last_date_str is not None):
                print(' Computing cutting indices...')
                if first_date_str is not None:
                    first_time_mask = first_time_data >= first_date_seconds
                if last_date_str is not None:
                    if first_date_str is not None:
                        first_time_mask = np.logical_and(first_time_mask, first_time_data <= last_date_seconds)
                    else:
                        first_time_mask = first_time_data <= last_date_seconds
            else:
                first_time_mask = np.ones(first_time_data.shape[0], dtype=bool)

            if np.invert(first_time_mask).all():
                time.sleep(sleep_time)
                print(' Warning: device ' + device_platform_code + ' (' + device_name + ') '
                      + variable_standard_name + ' variable has been rejected because '
                      ' first dataset has empty intersection with the selected time period.',
                      file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue

            first_time_indices = np.where(first_time_mask)[0]
            first_time_data = first_time[first_time_indices]

            try:
                second_file = [second_field_dir + file for file in os.listdir(second_field_dir) if
                               device_platform_code + '_' + variable_standard_name + '_' in file][0]
            except (OSError, IndexError):
                time.sleep(sleep_time)
                print(' Warning: device ' + device_platform_code + ' (' + device_name + ') '
                      ' dataset in second directory not found. Skipping.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue
            second_sampling_time_seconds = time_calc.time_calc(second_file, verbose=False)
            second_sampling_time_days = second_sampling_time_seconds // 86400
            second_sampling_time_modulus = second_sampling_time_seconds - second_sampling_time_days * 86400
            second_sampling_time = time.strftime('%H:%M:%S', time.gmtime(second_sampling_time_modulus))
            if second_sampling_time_seconds == 1:
                print(' most representative sampling time: 1 second')
            elif second_sampling_time_seconds < 60:
                print(' most representative sampling time: ' + second_sampling_time + ' seconds')
            elif second_sampling_time_seconds == 60:
                print(' most representative sampling time: ' + second_sampling_time + ' minute')
            elif second_sampling_time_seconds < 3600:
                print(' most representative sampling time: ' + second_sampling_time + ' minutes')
            elif second_sampling_time_seconds == 3600:
                print(' most representative sampling time: ' + second_sampling_time + ' hour')
            elif second_sampling_time_seconds < 86400:
                print(' most representative sampling time: ' + second_sampling_time + ' hours')
            elif second_sampling_time_seconds == 86400:
                print(' most representative sampling time: 1 day')

            if first_sampling_time_seconds != second_sampling_time_seconds:
                time.sleep(sleep_time)
                print(' Error: mixed sampling times. Cannot compute evaluation.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return

            if first_sampling_time_seconds == 3600:
                record_type = 'hm'
                out_mean_str = 'Hourly Mean'
            elif first_sampling_time_seconds == 86400:
                record_type = 'dm'
                out_mean_str = 'Daily Mean'
            else:
                time.sleep(sleep_time)
                print(' Error: only handled daily and hourly evaluation for now. Sorry...', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
            second_data = netCDF4.Dataset(second_file, mode='r')
            print(' Loading ' + second_file + ' data file...')
            second_longitude = second_data.variables['lon']
            second_longitude_data = second_longitude[...]
            second_latitude = second_data.variables['lat']
            second_latitude_data = second_latitude[...]
            second_depth = second_data.variables['depth']
            second_depth_data = second_depth[...]
            second_time = second_data.variables['time']
            second_time_reference = second_time.units
            if 'days' in second_time_reference:
                second_time_data = np.round(second_time[...] * 86400.)
            elif 'seconds' in second_time_reference:
                second_time_data = np.round(second_time[...])
            second_time_reference = second_time_reference[second_time_reference.find('since ') + len('since '):]
            second_reference_data = abs(calendar.timegm(time.strptime(second_time_reference, '%Y-%m-%dT%H:%M:%SZ')))
            second_time_data += - second_reference_data
            if (first_date_str is not None) or (last_date_str is not None):
                print(' Computing cutting indices...')
                if first_date_str is not None:
                    second_time_mask = second_time_data >= first_date_seconds
                if last_date_str is not None:
                    if first_date_str is not None:
                        second_time_mask = np.logical_and(second_time_mask, second_time_data <= last_date_seconds)
                    else:
                        second_time_mask = second_time_data <= last_date_seconds
            else:
                second_time_mask = np.ones(second_time_data.shape[0], dtype=bool)

            if np.invert(second_time_mask).all():
                time.sleep(sleep_time)
                print(' Warning: device ' + device_platform_code + ' (' + device_name + ') '
                      + variable_standard_name + ' variable has been rejected because '
                      ' second dataset has empty intersection with the selected time period.',
                      file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue

            second_time_indices = np.where(second_time_mask)[0]
            second_time_data = second_time[second_time_indices]

            intersection_time_data = np.intersect1d(first_time_data, second_time_data)
            if intersection_time_data.shape[0] == 0:
                time.sleep(sleep_time)
                print(' Warning: device ' + device_platform_code + ' (' + device_name + ') '
                      ' second variable has empty intersection with first variable.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue

            out_time_data = np.union1d(first_time_data, second_time_data)

            first_index_time_data = [index for index in range(out_time_data.shape[0]) if
                                     out_time_data[index] in first_time_data]
            second_index_time_data = [index for index in range(out_time_data.shape[0]) if
                                      out_time_data[index] in second_time_data]

            out_depth_data = np.intersect1d(first_depth_data, second_depth_data)
            if out_depth_data.shape[0] == 0:
                time.sleep(sleep_time)
                print(' Warning: device ' + device_platform_code + ' (' + device_name + ') '
                      ' first and second datasets depth coordinates do not intercept.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue

            first_index_depth_data = [index for index in range(first_depth_data.shape[0])
                                      if first_depth_data[index] in out_depth_data]
            second_index_depth_data = [index for index in range(second_depth_data.shape[0])
                                       if second_depth_data[index] in out_depth_data]

            first_variable_name =\
                find_variable_name.find_variable_name(first_file, 'standard_name', variable_standard_name,
                                                      verbose=False)
            first_variable = first_data.variables[first_variable_name]
            first_variable_data =\
                np.ma.masked_all(shape=(out_time_data.shape[0], out_depth_data.shape[0],)
                                 + first_variable.shape[2:], dtype=first_variable.datatype)
            extracted_first_variable_data = first_variable[first_time_indices, ...]
            extracted_first_variable_data = extracted_first_variable_data[:, first_index_depth_data, ...]
            first_variable_data[first_index_time_data, :, ...] = extracted_first_variable_data

            if not np.ma.is_masked(first_variable_data):
                first_variable_data = np.ma.array(first_variable_data,
                                                  mask=np.zeros(shape=first_variable_data.shape, dtype=bool),
                                                  fill_value=out_fill_value, dtype=first_variable_data.dtype)

            first_variable_data = np.ma.masked_where(np.abs(first_variable_data) > out_fill_value / 1000,
                                                     first_variable_data)

            if (('insitu-data' in first_file) or ('model-data' in second_file)) or \
                    (('model-data' in first_file) or ('insitu-data' in second_file)):
                if variable_standard_name == 'sea_surface_height_above_sea_level':
                    first_variable_data -= np.ma.mean(first_variable_data)

            if first_variable_data.mask.all():
                time.sleep(sleep_time)
                print(' Warning: device ' + device_platform_code + ' (' + device_name + ') '
                      + variable_standard_name + ' variable has been rejected because'
                      ' all first data is missing for the selected time period.',
                      file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue
            second_variable_name = \
                find_variable_name.find_variable_name(second_file, 'standard_name', variable_standard_name,
                                                      verbose=False)
            second_variable = second_data.variables[second_variable_name]
            second_variable_data = \
                np.ma.masked_all(shape=(out_time_data.shape[0], out_depth_data.shape[0],)
                                 + second_variable.shape[2:], dtype=second_variable.datatype)
            extracted_second_variable_data = second_variable[second_time_indices, ...]
            extracted_second_variable_data = extracted_second_variable_data[:, second_index_depth_data, ...]
            second_variable_data[second_index_time_data, :, ...] = extracted_second_variable_data

            if not np.ma.is_masked(second_variable_data):
                second_variable_data = np.ma.array(second_variable_data,
                                                   mask=np.zeros(shape=second_variable_data.shape, dtype=bool),
                                                   fill_value=out_fill_value, dtype=second_variable_data.dtype)

            second_variable_data = np.ma.masked_where(np.abs(second_variable_data) > out_fill_value / 1000,
                                                      second_variable_data)

            if (('insitu-data' in first_file) or ('model-data' in second_file)) or \
                    (('model-data' in first_file) or ('insitu-data' in second_file)):
                if variable_standard_name == 'sea_surface_height_above_sea_level':
                    second_variable_data -= np.ma.mean(second_variable_data)

            if second_variable_data.mask.all():
                time.sleep(sleep_time)
                print(' Warning: device ' + device_platform_code + ' (' + device_name + ') '
                      + variable_standard_name + ' variable has been rejected because'
                      ' all second data is missing for the selected time period.',
                      file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue

            statistical_bias_variable_data = first_variable_data - second_variable_data

            if statistical_bias_variable_data.mask.all():
                time.sleep(sleep_time)
                print(' Warning: device ' + device_platform_code + ' (' + device_name + ') ' + variable_standard_name +
                      ' variable has been rejected because the two time series do not intercept.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue

            root_mean_square_error_variable_data = np.sqrt(np.abs(statistical_bias_variable_data) ** 2)

            rmse_profile_data =\
                np.ma.mean(root_mean_square_error_variable_data, axis=0)
            bias_profile_data = np.ma.mean(statistical_bias_variable_data, axis=0)

            print('  Building device ' + device_platform_code + ' (' + device_name + ')'
                  ' comparison dataset for ' + variable_standard_name + ' field...')

            # Create output dataset
            out_field_dir = out_dir + '/' + record_type + '/' + variable_standard_name + '/'
            if not os.path.exists(out_field_dir):
                print(' Creating ' + variable_standard_name + ' field folder...')
                os.makedirs(out_field_dir)
                print(' -------------------------')
            out_file = out_field_dir + '/evaluation_' + device_platform_code + \
                '_' + variable_standard_name + '_' + record_type + '.nc'
            out_data = netCDF4.Dataset(out_file, mode='w', format='NETCDF4')
            # Create output dimensions, dimension variables and set attributes
            for dimension_name in second_data.dimensions:
                if dimension_name == 'depth':
                    out_data.createDimension(dimension_name, out_depth_data.shape[0])
                else:
                    out_data.createDimension(dimension_name, first_data.dimensions[dimension_name].size
                                             if not first_data.dimensions[dimension_name].isunlimited() else None)

            out_dimension_variables = [first_title_str + '_lon', first_title_str + '_lat',
                                       second_title_str + '_lon', second_title_str + '_lat', 'depth', 'time']
            # Create new variables in output dataset and set attributes
            for variable_name in out_dimension_variables:
                if variable_name in [first_title_str + '_lon', first_title_str + '_lat']:
                    in_variable = first_data.variables[variable_name.replace(first_title_str + '_', '')]
                    out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                           dimensions=in_variable.dimensions)
                    out_variable[...] = in_variable[...]
                    out_variable.variable_type = 'From ' + first_title_str + ' dataset'
                elif variable_name in [second_title_str + '_lon', second_title_str + '_lat']:
                    in_variable = second_data.variables[variable_name.replace(second_title_str + '_', '')]
                    out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                           dimensions=in_variable.dimensions)
                    out_variable[...] = in_variable[...]
                    out_variable.variable_type = 'From ' + second_title_str + ' dataset'
                elif variable_name == 'depth':
                    in_variable = first_data.variables[variable_name]
                    out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                           dimensions=in_variable.dimensions)
                    out_variable[...] = out_depth_data
                    out_variable.variable_type = 'Intersected depth variables'
                elif variable_name == 'time':
                    in_variable = first_data.variables[variable_name]
                    out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                           dimensions=in_variable.dimensions,
                                                           zlib=True, complevel=1)
                    out_variable[...] = out_time_data + out_reference_data
                    out_variable.variable_type = 'Combined time variables'
                variable_attributes = [attribute for attribute in in_variable.ncattrs() if
                                       attribute not in '_FillValue']
                out_variable.setncatts(
                    {attribute: in_variable.getncattr(attribute) for attribute in variable_attributes})

            out_time_bounds = out_data.createVariable('time_bounds', 'f8',
                                                      dimensions=('time', 'axis_nbounds'), zlib=True, complevel=1)
            out_time_bounds.variable_type = 'Combined time variables'

            if record_type == 'dm':
                out_time_bounds[..., 0] = out_time_data - 43200. + out_reference_data
                out_time_bounds[..., 1] = out_time_data + 43200. + out_reference_data
            elif record_type == 'hm':
                out_time_bounds[..., 0] = out_time_data - 1800. + out_reference_data
                out_time_bounds[..., 1] = out_time_data + 1800. + out_reference_data
            out_time_bounds.units = 'seconds since ' + out_time_reference

            first_second_horizontal_distance =\
                out_data.createVariable(first_title_str + '_' + second_title_str + '_horizontal_distance',
                                        datatype=np.float32)
            first_second_horizontal_distance[...] = earth_distance(first_latitude_data, first_longitude_data,
                                                                   second_latitude_data, second_longitude_data)
            first_second_horizontal_distance.long_name =\
                first_title_str + ' - ' + second_title_str + ' Horizontal Distance'
            first_second_horizontal_distance.standard_name =\
                first_title_str + '_' + second_title_str + ' horizontal_distance'
            first_second_horizontal_distance.units = 'km'

            out_first_variable = out_data.createVariable(first_title_str + '_' + variable_standard_name,
                                                         datatype=first_variable.datatype,
                                                         dimensions=first_variable.dimensions,
                                                         fill_value=out_fill_value, zlib=True, complevel=1)
            out_first_variable[...] = first_variable_data
            out_first_variable.setncatts({attribute: second_variable.getncattr(attribute)
                                          for attribute in second_variable.ncattrs()
                                          if attribute not in '_FillValue'})
            out_first_variable.valid_min = np.float32(np.min(first_variable_data))
            out_first_variable.valid_max = np.float32(np.max(first_variable_data))
            out_first_variable.variable_type = 'From ' + first_title_str + ' dataset'

            out_second_variable = out_data.createVariable(second_title_str + '_' + variable_standard_name,
                                                          datatype=second_variable.datatype,
                                                          dimensions=second_variable.dimensions,
                                                          fill_value=out_fill_value, zlib=True, complevel=1)
            out_second_variable[...] = second_variable_data
            out_second_variable.setncatts({attribute: second_variable.getncattr(attribute)
                                           for attribute in second_variable.ncattrs()
                                           if attribute not in '_FillValue'})
            out_second_variable.valid_min = np.float32(np.min(second_variable_data))
            out_second_variable.valid_max = np.float32(np.max(second_variable_data))
            out_second_variable.variable_type = 'From ' + second_title_str + ' dataset'

            root_mean_square_error_variable = \
                out_data.createVariable('root_mean_square_error',
                                        datatype=first_variable.datatype, dimensions=first_variable.dimensions,
                                        fill_value=out_fill_value, zlib=True, complevel=1)
            root_mean_square_error_variable[...] = root_mean_square_error_variable_data
            root_mean_square_error_variable.missing_value = np.float32(out_fill_value)
            root_mean_square_error_variable.units = first_variable.units
            root_mean_square_error_variable.standard_name = 'root_mean_square_error'
            root_mean_square_error_variable.long_name = \
                'Root Mean Square Error Between ' + first_title_str + ' And ' + second_title_str + ' Data'
            root_mean_square_error_variable.valid_min = \
                np.float32(np.min(root_mean_square_error_variable_data))
            root_mean_square_error_variable.valid_max = \
                np.float32(np.max(root_mean_square_error_variable_data))

            statistical_bias_variable =\
                out_data.createVariable('bias',
                                        datatype=first_variable.datatype, dimensions=first_variable.dimensions,
                                        fill_value=out_fill_value, zlib=True, complevel=1)
            statistical_bias_variable[...] = statistical_bias_variable_data
            statistical_bias_variable.missing_value = np.float32(out_fill_value)
            statistical_bias_variable.units = first_variable.units
            statistical_bias_variable.standard_name = 'bias'
            statistical_bias_variable.long_name = \
                'Statistical Bias Between ' + first_title_str + ' And ' + second_title_str + ' Data'
            statistical_bias_variable.valid_min = \
                np.float32(np.min(statistical_bias_variable_data))
            statistical_bias_variable.valid_max = \
                np.float32(np.max(statistical_bias_variable_data))

            rmse_profile = out_data.createVariable('rmse_profile', datatype='f4',
                                                   dimensions=('depth',), fill_value=out_fill_value)

            rmse_profile[...] = rmse_profile_data
            rmse_profile.units = first_variable.units
            rmse_profile.coordinates = 'depth'
            rmse_profile.standard_name = 'rmse_profile'
            rmse_profile.long_name = 'Root Mean Square Error Vertical Profile'
            if out_depth_data.shape[0] > 1:
                rmse_profile.valid_min = np.float32(np.min(rmse_profile_data))
                rmse_profile.valid_max = np.float32(np.max(rmse_profile_data))

            bias_profile = out_data.createVariable('bias_profile', datatype='f4',
                                                   dimensions=('depth',), fill_value=out_fill_value)
            bias_profile[...] = bias_profile_data
            bias_profile.units = first_variable.units
            bias_profile.coordinates = 'depth'
            bias_profile.standard_name = 'bias_profile'
            bias_profile.long_name = 'Statistical Bias Vertical Profile'
            if out_depth_data.shape[0] > 1:
                bias_profile.valid_min = np.float32(np.min(bias_profile_data))
                bias_profile.valid_max = np.float32(np.max(bias_profile_data))

            # Set global attributes
            out_data.device_name = probes_names[insitu_device]
            out_data.device_type = probes_types[insitu_device]
            out_data.wmo_platform_code = probes_wmo[insitu_device]
            out_data.device_sampling_time = device_field_sampling_time + ' hh:mm:ss'
            out_data.device_organisation = probes_organizations[insitu_device]
            out_data.source = 'SOURCE evaluation system'
            out_data.conventions = 'CF-1.6'
            try:
                out_data.title = first_variable.long_name + ' Profile - ' + out_mean_str
            except AttributeError:
                try:
                    out_data.title = second_variable.long_name + ' Profile - ' + out_mean_str
                except AttributeError:
                    out_data.title = out_mean_str

            out_data.close()
            # break  # to produce only one variable

        print(' -------------------------')
        print(' Device ' + device_platform_code + ' (' + device_name + ') completed.')
        print(' -------------------------')
        # break  # to produce only one device

    total_run_time = time.gmtime(calendar.timegm(time.gmtime()) - start_run_time)
    print(' Finished! Total elapsed time is: '
          + time.strftime('%H:%M:%S', total_run_time) + ' hh:mm:ss')


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_csv_dir = sys.argv[1]
        first_dir = sys.argv[2]
        second_dir = sys.argv[3]
        in_fields_standard_name_str = sys.argv[4]
        out_dir = sys.argv[5]

    except IndexError:
        in_csv_dir = None
        first_dir = None
        second_dir = None
        in_fields_standard_name_str = None
        out_dir = None

    try:
        first_date_str = sys.argv[6]
    except (IndexError, ValueError):
        first_date_str = None

    try:
        last_date_str = sys.argv[7]
    except (IndexError, ValueError):
        last_date_str = None

    try:
        first_title_str = sys.argv[8]
    except (IndexError, ValueError):
        first_title_str = None

    try:
        second_title_str = sys.argv[9]
    except (IndexError, ValueError):
        second_title_str = None

    insitu_evaluation(in_csv_dir, first_dir, second_dir, in_fields_standard_name_str, out_dir,
                      first_date_str, last_date_str, first_title_str, second_title_str)
