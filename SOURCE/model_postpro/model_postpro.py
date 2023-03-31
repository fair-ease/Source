# -*- coding: utf-8 -*-
import sys
import os
import shlex
import shutil
import numpy as np
import netCDF4
import pandas as pd
import time
import calendar
from SOURCE.model_postpro import model_datasets_concatenator, vertical_interpolation
from SOURCE import ptmp_to_temp

# Global variables
sleep_time = 0.1  # seconds

model_standard_names = [['sea_water_temperature', 'sea_water_potential_temperature', 'temperature'],
                        ['sea_water_practical_salinity', 'sea_water_salinity', 'salinity']]


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def model_postpro(in_csv_dir=None, in_dir=None, in_fields_standard_name_str=None, work_dir=None, out_dir=None,
                  grid_observation_distance=None, mesh_mask_file=None, first_date_str=None, last_date_str=None,
                  concatenate_datasets_switch=True, vertical_interpolation_switch=True, verbose=True):
    """
    Script to post process model dataset to insitu devices CSV list.

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

        2) Model directories with datasets in netCDF format,
            daily or hourly, post processed or not, divided by field standard_name string passed before;

        3) Input variables standard_name attributes to process space separated string
            (for example: "sea_water_temperature sea_water_practical_salinity", please read CF conventions standard name
            table to find the correct strings to insert);

        4) Working directory;

        5) Output post processed datasets directory;

        6) Grid-to-observation maximum acceptable distance (km);

        7) Input model mesh mask file, with the same dimensions (so must be cut if output are cut);
            (OPTIONAL, default: to not use mesh mask file).
            IMPORTANT NOTE: If mesh mask file is unspecified, remember in this case to provide model data
            input files with already correctly filled points!!!

        8) First date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format (OPTIONAL);

        9) Last date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS (OPTIONAL);

        10) Concatenate datasets switch
            (OPTIONAL, if False only vertical interpolation on already produced time series will be computed);

        11) Vertical interpolation switch (OPTIONAL, if False only model datasets concatenation will be computed);

        12) verbosity switch (OPTIONAL).

    Output files are in netCDF-4 format, divided by device name and original or post processed field, containing:

        1) model nearest latitude;
        2) model nearest longitude;
        3) in situ ported model depths;
        4) time counter and boundaries;
        6) model_data time series;
        7) global attributes containing original datasets and post process specs.

    Written Sep 13, 2017 by Paolo Oliveri
    """
    if __name__ == '__main__':
        return
    start_run_time = calendar.timegm(time.gmtime())
    print('-------------------------' + ' ' + __file__ + ' -------------------------')
    print(' Local time is ' + time.strftime("%a %b %d %H:%M:%S %Z %Y", time.gmtime()))
    print(' -------------------------')
    print(' Script to post process model data to in situ platform locations.')
    print(' -------------------------')
    if in_csv_dir is None or in_dir is None or in_fields_standard_name_str is None or work_dir is None or \
            out_dir is None or grid_observation_distance is None:
        time.sleep(sleep_time)
        print(' ERROR: 6 of 12 maximum arguments (6 optionals) not provided.', file=sys.stderr)
        print(' 1) In situ information CSV directory;', file=sys.stderr)
        print(' 2) Model data input directory;', file=sys.stderr)
        print(' 3) Input fields standard_name space separated string to process'
              ' (for example: "sea_water_temperature sea_water_practical_salinity");', file=sys.stderr)
        print(' 4) Working directory;', file=sys.stderr)
        print(' 5) Model data output directory;', file=sys.stderr)
        print(' 6) Grid-to-observation maximum acceptable distance (km)', file=sys.stderr)
        print(' 7) (optional) Mesh mask file '
              ' default: not provided, filled points are taken in the model data input datasets);', file=sys.stderr)
        print(' 8) (optional) First date to evaluate in YYYYMMDD format'
              ' (default: first recorded date for each device);', file=sys.stderr)
        print(' 9) (optional) Last date to evaluate in YYYYMMDD format'
              ' (default: last recorded date for each device);', file=sys.stderr)
        print(' 10) (optional) Datasets concatenation switch (True or False) (default: True)'
              ' NOTE: if False only vertical interpolation on already produced time series will be computed);',
              file=sys.stderr)
        print(' 11) (optional) Vertical interpolation switch (True or False) (default: True)'
              ' NOTE: if False only model datasets concatenation will be computed);',
              file=sys.stderr)
        print(' 12) (optional) Verbosity switch (True or False) (default: True).', file=sys.stderr)
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

    if (mesh_mask_file == 'None') or (mesh_mask_file == ''):
        mesh_mask_file = None

    print(' In situ information CSV directory = ' + in_csv_dir)
    print(' Base per model data input directory or location ported datasets directory'
          ' (divided by standard_names or not) = ' + in_dir)
    print(' Input variables to process standard_name string = ' + in_fields_standard_name_str)
    print(' Working directory = ' + work_dir)
    print(' Output directory = ' + out_dir)
    print(' Grid-to-observation maximum distance (km) = ' + str(grid_observation_distance))
    print(' Mesh mask file = ' + str(mesh_mask_file) +
          ' (if None filled points will be taken in the model data input datasets)')
    print(' First date to process = ' + str(first_date_str) +
          ' (if None it will be the first available date)')
    print(' Last date to process = ' + str(last_date_str) +
          ' (if None it will be the last available date)')
    print(' Concatenate datasets switch = ' + str(concatenate_datasets_switch))
    print(' Vertical interpolation switch = ' + str(vertical_interpolation_switch))
    print(' verbosity switch = ' + str(verbose))
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

    if not os.path.exists(work_dir):
        print(' Creating working directory.')
        print(' -------------------------')
        os.makedirs(work_dir)
    if not os.listdir(work_dir):
        pass
    else:
        time.sleep(sleep_time)
        print(' Warning: existing files in working directory.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
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
    platform_codes_list = [platform_code for platform_code in probes_platform_codes]
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
    probes_longitudes = [longitudes.split(';') for longitudes in in_probes_data[:, 7]]
    probes_latitudes = [latitudes.split(';') for latitudes in in_probes_data[:, 8]]
    probes_depths = \
        [[depths.split(' ') for depths in split_depths.split(';')] for split_depths in in_probes_data[:, 12]]

    temperature_presence = False
    for test_standard_name in in_fields_standard_name_list:
        test_standard_names = list()
        for row in range(len(model_standard_names)):
            try:
                test_standard_name_index = model_standard_names[row].index(test_standard_name)
                test_standard_names = model_standard_names[row]
                break
            except ValueError:
                continue
        if 'sea_water_temperature' in test_standard_names:
            temperature_presence = True
            break

    # Model daily OR hourly datasets concatenation part
    if concatenate_datasets_switch:
        location_ported_standard_names_list = list()
        for variable_number in range(len(in_fields_standard_name_list)):
            model_standard_name = in_fields_standard_name_list[variable_number]
            test_standard_names = list()
            for row in range(len(model_standard_names)):
                try:
                    in_standard_name_index = model_standard_names[row].index(model_standard_name)
                    test_standard_names = model_standard_names[row]
                    break
                except ValueError:
                    continue
            for insitu_probe in range(in_probes_data.shape[0]):
                probe_platform_code = probes_platform_codes[insitu_probe]
                probe_standard_names = probes_standard_names[insitu_probe]
                probe_longitude_values = probes_longitudes[insitu_probe]
                probe_latitude_values = probes_latitudes[insitu_probe]
                try:
                    insitu_standard_name = list(set(test_standard_names).intersection(probe_standard_names))[0]
                    # Sea water potential temperature renaming
                    if insitu_standard_name == 'sea_water_temperature':
                        location_ported_standard_name = 'sea_water_potential_temperature'
                    else:
                        location_ported_standard_name = insitu_standard_name
                except IndexError:
                    if ('sea_water_practical_salinity' in test_standard_names) and temperature_presence:
                        # Force program to create temperature and salinity datasets to compute water temperature field
                        insitu_standard_name = 'sea_water_temperature'
                        location_ported_standard_name = 'sea_water_practical_salinity'
                    else:
                        continue

                if location_ported_standard_name not in location_ported_standard_names_list:
                    location_ported_standard_names_list.append(location_ported_standard_name)
                    work_csv_file = work_dir + '/' + location_ported_standard_name + '_locations.csv'
                    print(' -------------------------')
                    print(' Building ' + location_ported_standard_name + ' locations CSV file...')
                    work_csv_data = np.empty(shape=(1, 3), dtype=object)
                    work_csv_data[0, :] = ['platform_code', 'Longitude', 'Latitude']
                    np.savetxt(work_csv_file, work_csv_data, fmt='"%s"', delimiter=',', comments='')

                try:
                    variable_name_index = probe_standard_names.index(insitu_standard_name)
                except ValueError:
                    continue
                probe_longitude = probe_longitude_values[variable_name_index]
                probe_latitude = probe_latitude_values[variable_name_index]

                work_csv_line = np.array([probe_platform_code, probe_longitude, probe_latitude], dtype=object)
                work_csv_data = np.append(work_csv_data, work_csv_line[np.newaxis, :], axis=0)
                np.savetxt(work_csv_file, work_csv_data, fmt='"%s"', delimiter=',', comments='')
            if work_csv_data.shape[0] == 1:
                time.sleep(sleep_time)
                print(' Warning. No probes with standard_name ' + location_ported_standard_name +
                      ' in input probes CSV file.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue

            # break  # to post process only one field

        for location_ported_standard_name in location_ported_standard_names_list:
            run_time = calendar.timegm(time.gmtime())
            work_csv_file = work_dir + '/' + location_ported_standard_name + '_locations.csv'
            for variable_number in range(len(in_fields_standard_name_list)):
                model_standard_name = in_fields_standard_name_list[variable_number]
                test_standard_names = list()
                for row in range(len(model_standard_names)):
                    try:
                        in_standard_name_index = model_standard_names[row].index(model_standard_name)
                        test_standard_names = model_standard_names[row]
                        break
                    except ValueError:
                        continue
                if location_ported_standard_name in test_standard_names:
                    break
                else:
                    continue

            in_field_dir = in_dir + '/' + model_standard_name + '/'
            try:
                in_datasets_list = \
                    [in_field_dir + '/' + file for file in os.listdir(in_field_dir) if file.endswith('.nc')]
                if len(in_datasets_list) == 0:
                    in_datasets_list = [in_dir + '/' + file for file in os.listdir(in_dir) if file.endswith('.nc')]
            except FileNotFoundError:
                in_datasets_list = [in_dir + '/' + file for file in os.listdir(in_dir) if file.endswith('.nc')]
            if len(in_datasets_list) == 0:
                time.sleep(sleep_time)
                print(' Warning. Wrong or empty ' + model_standard_name + ' input directory.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue
            location_ported_dir = work_dir + '/location-ported/' + location_ported_standard_name + '/'
            if not os.path.exists(location_ported_dir):
                print(' -------------------------')
                print(' Creating ' + location_ported_dir + ' field folder...')
                os.makedirs(location_ported_dir)
            print(' -------------------------')
            print(' Building ' + location_ported_standard_name + ' location ported datasets...')
            model_datasets_concatenator.model_datasets_concatenator(in_datasets_list, model_standard_name,
                                                                    work_csv_file, location_ported_dir,
                                                                    grid_observation_distance,
                                                                    location_ported_standard_name, mesh_mask_file,
                                                                    first_date_str, last_date_str, verbose=verbose)

            time_diff = time.gmtime(calendar.timegm(time.gmtime()) - run_time)
            print(' -------------------------')
            print(' Output field ' + location_ported_standard_name + ' completed.'
                                                                     ' ETA is ' + time.strftime('%H:%M:%S', time_diff))
            print(' -------------------------')
    else:
        location_ported_standard_names_list = in_fields_standard_name_list

    # Derived temperature field part
    if ('sea_water_potential_temperature' in location_ported_standard_names_list) and \
            ('sea_water_practical_salinity' in location_ported_standard_names_list):
        temperature_standard_name = 'sea_water_potential_temperature'
        out_temperature_standard_name = 'sea_water_temperature'
        salinity_standard_name = 'sea_water_practical_salinity'
        location_ported_standard_names_list =\
            [out_temperature_standard_name if standard_name == temperature_standard_name else standard_name
             for standard_name in location_ported_standard_names_list]
        if concatenate_datasets_switch:
            temperature_field_dir = work_dir + '/location-ported/' + temperature_standard_name + '/'
            salinity_field_dir = work_dir + '/location-ported/' + salinity_standard_name + '/'
            derived_temperature_field_dir = work_dir + '/location-ported/' + out_temperature_standard_name + '/'
        else:
            temperature_field_dir = in_dir + '/' + temperature_standard_name + '/'
            salinity_field_dir = in_dir + '/' + salinity_standard_name + '/'
            derived_temperature_field_dir = in_dir + '/' + out_temperature_standard_name + '/'
        try:
            temperature_field_list =\
                [temperature_field_dir + file for file in os.listdir(temperature_field_dir) if file.endswith('.nc')]
        except FileNotFoundError:
            time.sleep(sleep_time)
            print(' Error. Wrong or empty potential temperature location ported directory.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return
        try:
            salinity_field_list =\
                [salinity_field_dir + file for file in os.listdir(salinity_field_dir) if file.endswith('.nc')]
        except FileNotFoundError:
            time.sleep(sleep_time)
            print(' Error. Wrong or empty salinity location ported directory.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return
        for temperature_file in temperature_field_list:
            salinity_file =\
                temperature_file.replace(temperature_standard_name, salinity_standard_name)
            if not os.path.isfile(salinity_file):
                time.sleep(sleep_time)
                print(' Error. ' + salinity_file +
                      ' location ported salinity dataset is not present.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
            if not os.path.exists(derived_temperature_field_dir):
                print(' Creating ' + derived_temperature_field_dir + ' field folder...')
                os.makedirs(derived_temperature_field_dir)
            derived_temperature_field_file =\
                derived_temperature_field_dir + os.path.basename(temperature_file)\
                .replace(temperature_standard_name, out_temperature_standard_name)
            ptmp_to_temp.ptmp_to_temp(temperature_file, salinity_file, derived_temperature_field_file)
            # break  # To post process only the first file in the list

    # Vertical interpolation and output production part
    for variable_number in range(len(location_ported_standard_names_list)):
        run_time = calendar.timegm(time.gmtime())
        location_ported_standard_name = location_ported_standard_names_list[variable_number]
        test_standard_names = list()
        for row in range(len(model_standard_names)):
            try:
                test_standard_name_index = model_standard_names[row].index(location_ported_standard_name)
                test_standard_names = model_standard_names[row]
                break
            except ValueError:
                continue
        if concatenate_datasets_switch:
            in_field_dir = work_dir + '/location-ported/' + location_ported_standard_name + '/'
        else:
            in_field_dir = in_dir + '/' + location_ported_standard_name + '/'
        try:
            location_ported_list = \
                [in_field_dir + '/' + file for file in os.listdir(in_field_dir) if file.endswith('.nc')]
            if not concatenate_datasets_switch and len(location_ported_list) == 0:
                location_ported_list = [in_dir + '/' + file for file in os.listdir(in_dir) if file.endswith('.nc')]
        except FileNotFoundError:
            if not concatenate_datasets_switch:
                location_ported_list = [in_dir + '/' + file for file in os.listdir(in_dir) if file.endswith('.nc')]
            else:
                time.sleep(sleep_time)
                print(' Warning. Wrong or empty ' + location_ported_standard_name + ' location ported directory.',
                      file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue
        if len(location_ported_list) == 0:
            time.sleep(sleep_time)
            print(' Warning. Wrong or empty ' + location_ported_standard_name + ' location ported directory.',
                  file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            continue
        for location_ported_file in location_ported_list:
            location_ported_data = netCDF4.Dataset(location_ported_file, mode='r')
            try:
                location_ported_time = location_ported_data.variables['time']
            except KeyError:
                try:
                    location_ported_time = location_ported_data.variables['time_counter']
                except KeyError:
                    time.sleep(sleep_time)
                    print(' Error: missing record variable.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(' -------------------------')
                    return
            location_ported_time_stamps = pd.to_datetime(location_ported_time[...] * 1.e9)
            location_ported_data.close()
            location_ported_time_step = location_ported_time_stamps[-1] - location_ported_time_stamps[-2]
            if location_ported_time_step == pd.Timedelta('1 day'):
                out_record_type = 'dm'
            elif location_ported_time_step == pd.Timedelta('1 hour'):
                out_record_type = 'hm'
            out_record_dir = out_dir + '/' + out_record_type + '/'
            if not os.path.exists(out_record_dir):
                print(' Creating ' + out_record_dir + ' field folder...')
                os.makedirs(out_record_dir)
            try:
                location_ported_probe_index =\
                    [platform_codes_list.index(platform_code) for platform_code in platform_codes_list
                     if platform_code in location_ported_file][0]
            except IndexError:
                continue
            probe_platform_code = probes_platform_codes[location_ported_probe_index]
            probe_standard_names = probes_standard_names[location_ported_probe_index]
            probe_depths_values = probes_depths[location_ported_probe_index]
            try:
                insitu_standard_name = list(set(test_standard_names).intersection(probe_standard_names))[0]
            except IndexError:
                if ('sea_water_practical_salinity' in test_standard_names) and temperature_presence:
                    # Force program to create all temperature and salinity datasets to compute water temperature field
                    insitu_standard_name = 'sea_water_temperature'
                # Create sea water eastward_sea_water_velocity and northward_sea_water_velocity fields
                # also if in situ variables are sea_water_speed, direction_of_sea_water_velocity instead of them
                elif 'eastward_sea_water_velocity' in test_standard_names:
                    insitu_standard_name = 'sea_water_speed'
                elif 'northward_sea_water_velocity' in test_standard_names:
                    insitu_standard_name = 'direction_of_sea_water_velocity'
                else:
                    continue
            try:
                variable_name_index = probe_standard_names.index(insitu_standard_name)
            except ValueError:
                continue
            insitu_depth_data = probe_depths_values[variable_name_index]
            depth_array_str = ' '.join(insitu_depth_data)
            out_field_dir = out_record_dir + location_ported_standard_name + '/'
            if not os.path.exists(out_field_dir):
                print(' Creating ' + out_field_dir + ' field folder...')
                os.makedirs(out_field_dir)
            if vertical_interpolation_switch and (depth_array_str != 'floating') \
                    and ('surface' not in location_ported_standard_name):
                print(' -------------------------')
                print(' Interpolating ' + location_ported_file + ' dataset...')
                vertical_interpolated_field_dir = \
                    work_dir + '/vertical-interpolated/' + location_ported_standard_name + '/'
                if not os.path.exists(vertical_interpolated_field_dir):
                    print(' Creating ' + vertical_interpolated_field_dir + ' field folder...')
                    os.makedirs(vertical_interpolated_field_dir)
                vertical_interpolated_file = vertical_interpolated_field_dir +\
                    os.path.basename(location_ported_file).replace('location-ported', 'vertical-interpolated')
                vertical_interpolation.vertical_interpolation(location_ported_file, depth_array_str,
                                                              vertical_interpolated_file, verbose=verbose)
                processed_file = vertical_interpolated_file
                out_file_name = os.path.basename(vertical_interpolated_file).replace('_vertical-interpolated', '')
            else:
                if depth_array_str == 'floating':
                    time.sleep(sleep_time)
                    print(' Warning. The variable ' + location_ported_standard_name + ' of platform '
                          + probe_platform_code + ' is a floating sensor. '
                          ' Please analise in situ and location ported model data'
                          ' taking into account of time changing in situ depth coordinate.', file=sys.stderr)
                    time.sleep(sleep_time)
                processed_file = location_ported_file
                out_file_name = os.path.basename(location_ported_file).replace('_location-ported', '')
            print(' Copying ' + os.path.basename(processed_file) + ' to output directory.')
            shutil.copy2(processed_file, out_field_dir + out_file_name)

            # break  # To post process only one probe

        time_diff = time.gmtime(calendar.timegm(time.gmtime()) - run_time)
        print(' -------------------------')
        print(' Output field ' + location_ported_standard_name + ' completed.'
              ' ETA is ' + time.strftime('%H:%M:%S', time_diff))
        print(' -------------------------')

        # break  # to post process only one field

    total_run_time = time.gmtime(calendar.timegm(time.gmtime()) - start_run_time)
    print(' Finished! Total elapsed time is: '
          + str(int(np.floor(calendar.timegm(total_run_time) / 86400.))) + ' days '
          + time.strftime('%H:%M:%S', total_run_time) + ' hh:mm:ss')


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_csv_dir = sys.argv[1]
        in_dir = sys.argv[2]
        in_fields_standard_name_str = sys.argv[3]
        work_dir = sys.argv[4]
        out_dir = sys.argv[5]
        grid_observation_distance = sys.argv[6]
    except (IndexError, ValueError):
        in_csv_dir = None
        in_dir = None
        in_fields_standard_name_str = None
        work_dir = None
        out_dir = None
        grid_observation_distance = None

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
        concatenate_datasets_switch = string_to_bool(sys.argv[10])
    except (IndexError, ValueError):
        concatenate_datasets_switch = True

    try:
        vertical_interpolation_switch = string_to_bool(sys.argv[11])
    except (IndexError, ValueError):
        vertical_interpolation_switch = True

    try:
        verbose = string_to_bool(sys.argv[12])
    except (IndexError, ValueError):
        verbose = True

    model_postpro(in_csv_dir, in_dir, in_fields_standard_name_str, work_dir, out_dir,
                  grid_observation_distance, mesh_mask_file, first_date_str, last_date_str,
                  concatenate_datasets_switch, vertical_interpolation_switch, verbose)
