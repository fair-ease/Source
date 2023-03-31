# -*- coding: utf-8 -*-
import sys
import os
import shlex
import shutil
import numpy as np
import pandas as pd
import netCDF4
import time
import calendar
import datetime
import unidecode
from SOURCE import find_variable_name, pointwise_datasets_concatenator, time_check, time_calc
from SOURCE.obs_postpro import insitu_tac_platforms_finder, insitu_tac_timeseries_extractor,\
    data_information_calc, time_from_index, depth_calc, mean_variance_nc_variable, unique_values_nc_variable,\
    quality_check_applier

# Global variables
sleep_time = 0.1  # seconds
mean_duplicate_threshold = 0.0045  # degrees
# To prevent latitude-longitude error accumulation in average process
variance_duplicate_threshold = 1  # degrees
minimum_records_threshold = 2  # For time step calculation
minimum_record_days_threshold = 30  # For low time series segments removal, only creation mode

# Biscay Gulf limits
biscay_gulf_min_lon = -9.25
biscay_gulf_max_lon = -1.15
biscay_gulf_min_lat = 43.28
biscay_gulf_max_lat = 46

# Marmara Sea Limits
marmara_sea_min_lon = 26.83
marmara_sea_max_lon = 29.94
marmara_sea_min_lat = 40.3
marmara_sea_max_lat = 41.08

# Black Sea Limits
black_sea_min_lon = 27.32
black_sea_max_lon = 41.96
black_sea_min_lat = 40.91
black_sea_max_lat = 46.8

# Azov Sea Limits
azov_sea_min_lon = 34.81
azov_sea_max_lon = 39.3
azov_sea_min_lat = 45.28
azov_sea_max_lat = 47.28


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def insitu_tac_pre_processing(in_dir=None, in_fields_standard_name_str=None, work_dir=None, out_dir=None,
                              valid_qc_values=None, update_mode=None, first_date_str=None, last_date_str=None,
                              region_boundaries_str=None, med_sea_masking=False, in_instrument_types_str=None,
                              names_file=None, verbose=True):
    """
    Script to pre process CMEMS INSITU TAC insitu datasets
    from an already downloaded database with optional real time execution CSV table needed.

    Input arguments:

        1) In situ observations datasets directory;

        2) Input variables standard_name attributes to process space separated string
            (for example: "sea_water_temperature sea_water_practical_salinity", please read CF conventions standard name
            table to find the correct strings to insert);

        3) Base working directory;

        4) Output directory;

        5) Original DAC valid quality flags to use (space separated string, example: "0 1 2");

        6) Update mode execution switch (True or False) (OPTIONAL);

        7) Start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format (OPTIONAL);

        8) End date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS (OPTIONAL);

        9) Region longitude - latitude limits space separated string
            (min_lon, max_lon (deg E); min_lat, max_lat (deg N), OPTIONAL);

        10) Masking foreign seas switch for Mediterranean Sea processing (OPTIONAL);

        11) Input CMEMS "instrument type" metadata filter space separated string, OPTIONAL);
            (for example: "\"mooring\" \"coastal structure\"", please read CMEMS manual to properly write
            the attribute string, PLEASE put attributes with spaces with quotes to protect them from character escaping)

        12) Platform names CSV table (OPTIONAL) with two columns:
            a) Platform code;
            b) platform name;

        13) verbosity switch (OPTIONAL).

    Output:

        1) Devices information CSV file, with the sequent header:
            a) Device ID;
            b) Device Name.

        2) Organizations information CSV file, with the sequent header:
            a) Organization ID;
            b) Organization name;
            c) Organization Country (reverse searched from url extension, empty for generic url extensions);
            d) Organization Weblink (if available).

        3) Variables information CSV file, with the sequent header:
            a) Variable ID;
            b) Variable long_name attribute;;
            c) Variable standard_name attribute;
            d) Variable units.

        4) Probes information CSV file, with the sequent header:
            a) Probe ID;
            b) Probe SOURCE platform_code attribute;
            c) Probe name (if available or matches with probes_names.csv table);
            d) Probe WMO;
            e) Device type ID;
            f) Organization ID;
            g) Variable IDs;
            h) Per variable average longitudes;
            i) Per variable average latitudes;
            j) Per variable record starts;
            k) Per variable record ends;
            l) Per variable sampling times (ddd hh:mm:ss form);
            m) Per variable depth levels;
            n) Per variable quality controls information;
            o) Per variable ancillary notes;
            p) Probe link (if available).

        5) Processing information CSV file, with the sequent header:
            a) platform_code;
            b) institution;
            d) platform name;
            e) WMO;
            f) Platform type;
            g) Average longitude;
            h) Average latitude;
            i) processing information.

        6) Pre processed in situ files in netCDF-4 format, divided by variable standard names and
                DAC quality checked, containing:
            a) platform instantaneous latitude, longitude and depth dimension variables;
            b) platform time;
            c) DAC quality checked time series;
            d) global attributes containing original datasets and pre processing information.

    Written Dec 7, 2018 by Paolo Oliveri
    """
    if __name__ == '__main__':
        return
    start_run_time = calendar.timegm(time.gmtime())
    print('-------------------------' + ' ' + __file__ + ' -------------------------')
    print(' Local time is ' + time.strftime("%a %b %d %H:%M:%S %Z %Y", time.gmtime()))
    print(' -------------------------')
    print(' CMEMS IN SITU TAC pre processing NEAR REAL TIME / REPROCESSED observation files.')
    print(' -------------------------')
    if in_dir is None or in_fields_standard_name_str is None or work_dir is None or out_dir is None or \
            valid_qc_values is None:
        time.sleep(sleep_time)
        print(' ERROR: 5 of 13 maximum arguments (7 optionals) not provided.', file=sys.stderr)
        print(' 1) Input observations netCDF database directory;', file=sys.stderr)
        print(' 2) Input fields standard_name space separated string to process'
              ' (for example: "sea_water_temperature sea_water_practical_salinity");', file=sys.stderr)
        print(' 3) Working directory;', file=sys.stderr)
        print(' 4) Output directory;', file=sys.stderr)
        print(' 5) input variable valid qc values to consider (spaced valued string, for example: "0 1 2");',
              file=sys.stderr)
        print(' 6) update mode execution switch (True or False) (default: False);', file=sys.stderr)
        print(' 7) (optional) First date to evaluate in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: first recorded date for each platform);', file=sys.stderr)
        print(' 8) (optional) Last date to evaluate in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: last recorded date for each platform);', file=sys.stderr)
        print(' 9) (optional) Region longitude - latitude limits space separated string'
              ' (min_lon, max_lon (deg E); min_lat, max_lat (deg N), default: "-180 180 0 180" (all the Earth))',
              file=sys.stderr)
        print(' 10) (optional) Masking foreign seas switch for Mediterranean Sea processing switch (True or False)'
              ' (default: False).', file=sys.stderr)
        print(' 11) (optional) Input CMEMS "instrument type" metadata filter (space separated string, '
              ' for example: \'"mooring" "coastal structure"\');', file=sys.stderr)
        print(' 12) (optional) Platform CSV names table (default: internal file) with two columns:', file=sys.stderr)
        print('     a) Platform code;', file=sys.stderr)
        print('     b) platform name;', file=sys.stderr)
        print(' 13) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return

    try:
        valid_qc_values_list = [int(valid_qc_value) for valid_qc_value in valid_qc_values.split(' ')]
    except ValueError:
        time.sleep(sleep_time)
        print(' Error. Wrong valid qc values string.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

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

    if (region_boundaries_str is None) or (region_boundaries_str == 'None') or (region_boundaries_str == ''):
        region_boundaries_str = '-180 180 -90 90'

    # Box boundaries to process probes
    west_boundary = np.float32(region_boundaries_str.split(' ')[0])
    east_boundary = np.float32(region_boundaries_str.split(' ')[1])
    south_boundary = np.float32(region_boundaries_str.split(' ')[2])
    north_boundary = np.float32(region_boundaries_str.split(' ')[3])

    # Biscay Gulf masking condition (mask only if there is no connection between the Atlantic Ocean and the Biscay Gulf)
    biscay_gulf_masking = False
    if (west_boundary > biscay_gulf_min_lon) and (west_boundary < biscay_gulf_max_lon) and \
            (east_boundary > biscay_gulf_min_lon) and \
            (south_boundary < biscay_gulf_max_lat) and (north_boundary > biscay_gulf_min_lat) and med_sea_masking:
        biscay_gulf_masking = True

    # Marmara Sea Masking condition
    marmara_sea_masking = False
    if (west_boundary < marmara_sea_max_lon) and (east_boundary > marmara_sea_min_lon) and \
            (south_boundary < marmara_sea_max_lat) and (north_boundary > marmara_sea_min_lat) and med_sea_masking:
        marmara_sea_masking = True

    # Black Sea Masking condition
    black_sea_masking = False
    if (west_boundary < black_sea_max_lon) and (east_boundary > black_sea_min_lon) and\
            (south_boundary < black_sea_max_lat) and (north_boundary > marmara_sea_min_lat) and med_sea_masking:
        black_sea_masking = True

    # Azov Sea Masking condition
    azov_sea_masking = False
    if (west_boundary < azov_sea_max_lon) and (east_boundary > azov_sea_min_lon) and\
            (south_boundary < azov_sea_max_lat) and (north_boundary > azov_sea_min_lat) and med_sea_masking:
        azov_sea_masking = True

    if (names_file is None) or (names_file == 'None') or (names_file == ''):
        names_file = os.path.dirname(__file__) + '/probes_names.csv'

    print(' Input directory = ' + in_dir)
    print(' Input variables to process standard_name string = ' + in_fields_standard_name_str)
    print(' Working directory = ' + work_dir)
    print(' Output directory = ' + out_dir)
    print(' Valid qc values to consider = ' + valid_qc_values)
    print(' Update mode = ' + str(update_mode))
    print(' First date to process = ' + str(first_date_str) +
          ' (if None it will be the first available date on each platform)')
    print(' Last date to process = ' + str(last_date_str) +
          ' (if None it will be the last available date on each platform)')
    print(' Region boundary horizontal limits (min_lon, max_lon (deg E); min_lat, max_lat (deg N)) = '
          + region_boundaries_str)
    print(' Masking foreign seas switch for Med Sea processing switch = ' + str(med_sea_masking))
    print(' Input "instrument / type" metadata filter string = ' + str(in_instrument_types_str) +
          ' (if None all instruments will be processed)')
    print(' Input platform names CSV table = ' + names_file)
    print(' verbosity switch = ' + str(verbose))
    print(' -------------------------')
    print(' Starting process...')
    print(' -------------------------')

    if (in_fields_standard_name_str is None) or (in_fields_standard_name_str == 'None') or \
            (in_fields_standard_name_str == '') or len(in_fields_standard_name_str.split(' ')) < 1:
        time.sleep(sleep_time)
        print(' Error. Wrong input fields string.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    else:
        in_fields_standard_name_list = in_fields_standard_name_str.split(' ')

    if in_instrument_types_str is not None:
        if (in_instrument_types_str == 'None') or \
                (in_instrument_types_str == '') or len(in_instrument_types_str.split(' ')) < 1:
            time.sleep(sleep_time)
            print(' Error. Wrong input "instrument / gear type" metadata filter string.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return
        else:
            in_instrument_types_list = shlex.split(in_instrument_types_str)
    else:
        in_instrument_types_list = None

    file_list = [in_dir + '/' + file for file in os.listdir(in_dir) if file.endswith('.nc')]
    if not file_list:
        time.sleep(sleep_time)
        print(' Error. No processable files in input directory.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    file_list.sort()

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

    url_data = open(os.path.dirname(__file__) + '/url_countries.csv', 'rb')
    url_data = \
        pd.read_csv(url_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if url_data.ndim == 1:
        url_data = url_data[np.newaxis, :]
    url_extensions = url_data[:, 0]
    if url_extensions.shape[0] == 0:
        time.sleep(sleep_time)
        print(' Error. Empty url names CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    url_countries = url_data[:, 1]

    names_data = open(names_file, 'rb')
    names_data = \
        pd.read_csv(names_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if names_data.ndim == 1:
        names_data = names_data[np.newaxis, :]
    names_probes_names = names_data[:, 0]
    if names_probes_names.shape[0] == 0:
        time.sleep(sleep_time)
        print(' Error. Empty platform names CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    names_probes_longitudes = np.array(names_data[:, 1], dtype=np.float32)
    names_probes_latitudes = np.array(names_data[:, 2], dtype=np.float32)

    out_devices_file = out_dir + '/devices.csv'
    out_devices_data = np.empty(shape=(1, 2), dtype=object)
    out_devices_data[0, :] = ['id', 'name']
    print(' Writing output devices CSV file header...')
    np.savetxt(out_devices_file, out_devices_data, fmt='"%s"', delimiter=',', comments='')

    out_organizations_file = out_dir + '/organizations.csv'
    out_organizations_data = np.empty(shape=(1, 4), dtype=object)
    out_organizations_data[0, :] = ['id', 'name', 'country', 'link']
    print(' Writing output organizations CSV file header...')
    np.savetxt(out_organizations_file, out_organizations_data, fmt='"%s"', delimiter=',', comments='')

    out_variables_file = out_dir + '/variables.csv'
    out_variables_data = np.empty(shape=(1, 4), dtype=object)
    out_variables_data[0, :] = ['id', 'standard_name', 'long_name', 'units']
    print(' Writing output variables CSV file header...')
    np.savetxt(out_variables_file, out_variables_data, fmt='"%s"', delimiter=',', comments='')

    out_probes_file = out_dir + '/probes.csv'
    out_probes_data = np.empty(shape=(1, 16), dtype=object)
    out_probes_data[0, :] = ['id', 'platform_code', 'name', 'wmo', 'device_id', 'organization_id', 'variable_ids',
                             'longitudes', 'latitudes', 'record_starts', 'record_ends',
                             'sampling_times', 'depths', 'quality_controls', 'notes', 'link']
    print(' Writing output probes CSV file header...')
    np.savetxt(out_probes_file, out_probes_data, fmt='"%s"', delimiter=',', comments='')

    out_processing_file = out_dir + '/processing_information.csv'
    out_processing_data = np.empty(shape=(1, 9), dtype=object)
    out_processing_data[0, :] = ['file_name', 'platform_code', 'organization', 'name', 'wmo', 'device_id',
                                 'longitude', 'latitude', 'processing_information']
    print(' Writing processing information CSV file header...')
    np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',', comments='')

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

    progression_percentage_list = list()
    analyzed_list = list()
    device_id = 0
    organization_id = 0
    probe_id = 0
    in_variable_ids = np.arange(1, len(in_fields_standard_name_list) + 1)
    for in_file in file_list:
        completion_percentage = np.around(len(progression_percentage_list) / len(file_list) * 100, decimals=1)
        progression_percentage_list.append(in_file)
        processing_message = None
        not_analyzed_list = [dataset for dataset in file_list if dataset not in analyzed_list]
        in_data = netCDF4.Dataset(in_file, mode='r')
        platform_code = in_data.platform_code
        # if platform_code != '':
        #     continue
        print_prefix = ' (' + platform_code + ')'
        run_time = calendar.timegm(time.gmtime())
        if verbose:
            print(print_prefix + ' -------------------------')
            print(print_prefix + ' Local time is ' + time.strftime("%Y-%m-%d %H:%M:%S %Z", time.gmtime()))
            print(print_prefix + ' Processing completion percentage: ' + str(completion_percentage) + '%')
        print(print_prefix + ' -------------------------')
        print(print_prefix + ' input file: ' + in_file)
        print(print_prefix + ' -------------------------')
        print(print_prefix + ' platform code = \'' + platform_code + '\'')
        organization_name = unidecode.unidecode(in_data.institution.replace(',', ''))
        print(print_prefix + ' institution: ' + organization_name)
        try:
            edmo_code = in_data.institution_edmo_code
            print(print_prefix + ' EDMO_code: ' + str(edmo_code))
        except AttributeError:
            edmo_code = None
        try:
            organization_link = in_data.institution_references.replace(',', '')
            organization_extension = '.' + organization_link.split('.')[-1].replace("/", "")
            if organization_extension in url_extensions:
                url_index = np.where(url_extensions == organization_extension)[0][0]
                organization_country = url_countries[url_index]
            else:
                organization_country = ''
        except AttributeError:
            organization_link = ''
            organization_country = ''
        try:
            platform_name = in_data.platform_name
        except AttributeError:
            platform_name = ''
        if platform_name != '':
            print(print_prefix + ' platform name = \'' + platform_name + '\'')
        try:
            wmo = in_data.wmo_platform_code
        except AttributeError:
            wmo = ''
        if wmo == ' ':
            wmo = ''
        print(print_prefix + ' WMO platform code = \'' + wmo + '\'')
        try:
            probe_link = in_data.references
        except AttributeError:
            probe_link = ''
        if probe_link == ' ':
            probe_link = ''
        print(print_prefix + ' web references = \'' + probe_link + '\'')
        try:
            platform_type = in_data.source
        except AttributeError:
            platform_type = ''
        if (platform_type == '') or (platform_type == ' '):
            platform_type = 'undefined'
        print(print_prefix + ' Platform type = \'' + platform_type + '\'')
        record_dimension = None
        csv_platform_name = None
        [longitude_mean, longitude_variance] =\
            mean_variance_nc_variable.mean_variance_nc_variable(in_file, 'longitude', verbose=False)
        if longitude_mean > 180:
            longitude_mean -= 360
        [latitude_mean, latitude_variance] =\
            mean_variance_nc_variable.mean_variance_nc_variable(in_file, 'latitude', verbose=False)
        if latitude_mean > 90:
            latitude_mean -= 180
        for names_csv_row in range(names_data.shape[0]):
            csv_longitude = names_probes_longitudes[names_csv_row]
            csv_latitude = names_probes_latitudes[names_csv_row]
            if (np.abs(csv_longitude - longitude_mean) < mean_duplicate_threshold) and \
                    (np.abs(csv_latitude - latitude_mean) < mean_duplicate_threshold):
                csv_platform_name = names_probes_names[names_csv_row]
                print(print_prefix + ' detected CSV platform name = \'' + csv_platform_name + '\'')
                break
        print(print_prefix + ' mean longitude : ' + str(longitude_mean) + ' degrees west')
        print(print_prefix + ' longitude variance : ' + str(longitude_variance) + ' degrees')

        print(print_prefix + ' mean latitude : ' + str(latitude_mean) + ' degrees east')
        print(print_prefix + ' latitude variance : ' + str(latitude_variance) + ' degrees')
        out_processing_line = np.array([[os.path.basename(in_file), platform_code, organization_name,
                                         platform_name, wmo, platform_type,
                                         str(latitude_mean), str(latitude_mean)]])
        if in_file in analyzed_list:
            time.sleep(sleep_time)
            print(' Warning:' + print_prefix + ' already analyzed.', file=sys.stderr)
            time.sleep(sleep_time)
            print(print_prefix + ' -------------------------')
            processing_message = 'OK (concatenated)'
            out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
            out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
            np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',', comments='')
            continue
        if (in_instrument_types_list is not None) and (platform_type not in in_instrument_types_list):
            time.sleep(sleep_time)
            print(' Warning:' + print_prefix + ' in situ device type not in selected list.',
                  file=sys.stderr)
            time.sleep(sleep_time)
            print(print_prefix + ' -------------------------')
            processing_message = 'Device type not selected (' + in_instrument_types_str + ')'
            out_out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
            out_processing_data = np.append(out_processing_data, out_out_processing_line, axis=0)
            np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',', comments='')
            continue
        out_of_area = False
        if (not west_boundary < longitude_mean < east_boundary) or \
                (not south_boundary < latitude_mean < north_boundary):
            out_of_area = True
        if ((biscay_gulf_min_lon < longitude_mean < biscay_gulf_max_lon) and
                (biscay_gulf_min_lat < latitude_mean < biscay_gulf_max_lat)) and biscay_gulf_masking:
            out_of_area = True
        if ((marmara_sea_min_lon < longitude_mean < marmara_sea_max_lon) and
                (marmara_sea_min_lat < latitude_mean < marmara_sea_max_lat)) and marmara_sea_masking:
            out_of_area = True
        if ((black_sea_min_lon < longitude_mean < black_sea_max_lon) and
                (black_sea_min_lat < latitude_mean < black_sea_max_lat)) and black_sea_masking:
            out_of_area = True
        if ((azov_sea_min_lon < longitude_mean < azov_sea_max_lon) and
                (azov_sea_min_lat < latitude_mean < azov_sea_max_lat)) and azov_sea_masking:
            out_of_area = True
        if out_of_area:
            time.sleep(sleep_time)
            print(' Warning:' + print_prefix + ' in situ location is outside the selected area.',
                  file=sys.stderr)
            time.sleep(sleep_time)
            print(print_prefix + ' -------------------------')
            processing_message = 'Out of selected area (' + region_boundaries_str + ')'
            out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
            out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
            np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',', comments='')
            continue
        recorded_fields_standard_names = list()
        recorded_fields_long_names = list()
        true_fields_standard_names = list()
        true_fields_long_names = list()
        for variable in in_data.variables.keys():
            try:
                in_data_field_standard_name = in_data.variables[variable].standard_name
                recorded_fields_standard_names.append(in_data_field_standard_name)
            except AttributeError:
                continue
            try:
                in_data_field_long_name = in_data.variables[variable].long_name
            except AttributeError:
                in_data_field_long_name = ''
            recorded_fields_long_names.append(in_data_field_long_name)
            if (in_data_field_standard_name in in_fields_standard_name_list) and \
                    (in_data_field_standard_name not in true_fields_standard_names):
                true_fields_standard_names.append(in_data_field_standard_name)
                true_fields_long_names.append(in_data_field_long_name)
        recorded_data = [standard_name + ' (' + long_name + ')' for standard_name, long_name in
                         zip(recorded_fields_standard_names, recorded_fields_long_names) if standard_name not in
                         ['longitude', 'latitude', 'depth', 'time', 'sea_water_pressure', 'air_pressure']]
        print(print_prefix + ' Recorded ocean data :')
        for field_number in range(len(recorded_data)):
            recorded_variable = recorded_data[field_number]
            print(print_prefix + ' ' + str(field_number + 1) + ') ' + recorded_variable)
        if len(true_fields_standard_names) == 0:
            time.sleep(sleep_time)
            print(' Warning:' + print_prefix + ' does not contain any of the selected fields.', file=sys.stderr)
            time.sleep(sleep_time)
            print(print_prefix + ' -------------------------')
            in_data.close()
            processing_message = 'No fields available from input selection (' + in_fields_standard_name_str + ')'
            out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
            out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
            np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',', comments='')
            continue
        in_data.close()
        print(print_prefix + ' Searching all surroundings datasets matching this platform_code...')
        [concatenate_list, out_platform_code, out_platform_name, out_wmo,
            out_platform_type, out_organization_name, out_probe_link] = \
            insitu_tac_platforms_finder.insitu_tac_platforms_finder(not_analyzed_list,
                                                                    longitude_mean, latitude_mean,
                                                                    in_fields_standard_name_str,
                                                                    first_date_str, last_date_str, verbose=False)
        if not concatenate_list:
            time.sleep(sleep_time)
            print(' Warning:' + print_prefix + ' out of time range'
                  ' or too high horizontal variance in input file(s).', file=sys.stderr)
            time.sleep(sleep_time)
            print(print_prefix + ' -------------------------')
            processing_message = ' out of time range or too high horizontal variance in input file(s)'
            out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
            out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
            np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',',
                       comments='')
            continue
        analyzed_list += concatenate_list
        out_file_name = 'insitu-data_' + out_platform_code
        concatenated_file = work_dir + '/' + out_file_name + '_concatenated.nc'
        if len(concatenate_list) > 1:
            print(print_prefix + ' -------------------------')
            print(print_prefix + ' Generating concatenated dataset for all files with this platform_code.')
            print(print_prefix + ' To be concatenated files list:')
            for file_number in range(len(concatenate_list)):
                print(print_prefix + ' ' + str(file_number + 1) + ') ' +
                      os.path.basename(concatenate_list[file_number]))
            pointwise_datasets_concatenator.pointwise_datasets_concatenator(concatenate_list, concatenated_file,
                                                                            in_fields_standard_name_str,
                                                                            first_date_str, last_date_str,
                                                                            verbose=verbose)
            if not os.path.isfile(concatenated_file):
                time.sleep(sleep_time)
                print(' Warning:' + print_prefix + ' no data to concatenate for the selected period.', file=sys.stderr)
                time.sleep(sleep_time)
                print(print_prefix + ' -------------------------')
                processing_message = 'No data to concatenate for the period '\
                                     + str(first_date_str) + ' ' + str(last_date_str)
                out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
                out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
                np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',',
                           comments='')
                continue
            print(print_prefix + ' Analyzing concatenated dataset.')
            concatenated_data = netCDF4.Dataset(concatenated_file, mode='r')
            recorded_fields_standard_names = list()
            recorded_fields_long_names = list()
            true_fields_standard_names = list()
            true_fields_long_names = list()
            for variable in concatenated_data.variables.keys():
                try:
                    concatenated_data_field_standard_name = concatenated_data.variables[variable].standard_name
                    recorded_fields_standard_names.append(concatenated_data_field_standard_name)
                except AttributeError:
                    continue
                try:
                    concatenated_data_field_long_name = concatenated_data.variables[variable].long_name
                except AttributeError:
                    concatenated_data_field_long_name = ''
                recorded_fields_long_names.append(concatenated_data_field_long_name)
                if (concatenated_data_field_standard_name in in_fields_standard_name_list) and \
                        (concatenated_data_field_standard_name not in true_fields_standard_names):
                    true_fields_standard_names.append(concatenated_data_field_standard_name)
                    true_fields_long_names.append(concatenated_data_field_long_name)
            recorded_data = [standard_name + ' (' + long_name + ')' for standard_name, long_name in
                             zip(recorded_fields_standard_names, recorded_fields_long_names)
                             if standard_name not in ['longitude', 'latitude', 'depth', 'time']]
            print(print_prefix + ' concatenated dataset recorded ocean data :')
            for field_number in range(len(recorded_data)):
                recorded_variable = recorded_data[field_number]
                print(print_prefix + ' ' + str(field_number + 1) + ') ' + recorded_variable)
        elif len(concatenate_list) == 1:
            print(print_prefix + ' No other datasets found.')
            concatenated_file = in_file
        else:
            time.sleep(sleep_time)
            print(' Warning:' + print_prefix + ' no data or fields in the selected period.',
                  file=sys.stderr)
            time.sleep(sleep_time)
            print(print_prefix + ' -------------------------')
            processing_message = 'No data to concatenate for the period ' \
                                 + str(first_date_str) + ' ' + str(last_date_str)
            out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
            out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
            np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',',
                       comments='')
            continue
        concatenated_data = netCDF4.Dataset(concatenated_file, mode='r')

        try:
            depth_dimension_name = 'DEPTH'
            depth_dimension_length = len(concatenated_data.dimensions[depth_dimension_name])
        except KeyError:
            depth_dimension_length = 1
        print(print_prefix + ' input file depth levels: ' + str(depth_dimension_length))
        depth_variable_name = None
        pres_variable_name = None
        try:
            depth_variable_name = find_variable_name.find_variable_name(concatenated_file, 'standard_name', 'depth',
                                                                        verbose=False)
        except KeyError:
            pass
        try:
            pres_variable_name = find_variable_name.find_variable_name(concatenated_file, 'standard_name',
                                                                       'sea_water_pressure', verbose=False)
        except KeyError:
            pass
        if depth_variable_name is None and pres_variable_name is None:
            time.sleep(sleep_time)
            print(' Error. No depth or pressure variables in input file.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return
        for dimension in concatenated_data.dimensions:
            if concatenated_data.dimensions[dimension].isunlimited():
                record_dimension = dimension
                break
        if record_dimension is None:
            try:
                record_dimension = 'TIME'
                in_records_number = concatenated_data.dimensions[record_dimension].size
            except KeyError:
                try:
                    record_dimension = 'row'
                    in_records_number = concatenated_data.dimensions[record_dimension].size
                except KeyError:
                    time.sleep(sleep_time)
                    print(' Error. Record dimension not found in input file.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(' -------------------------')
                    return
        else:
            try:
                in_records_number = concatenated_data.dimensions[record_dimension].size
            except KeyError:
                time.sleep(sleep_time)
                print(' Error. Record dimension not found in input file.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
        concatenated_time_variable_name = find_variable_name.find_variable_name(concatenated_file, 'standard_name',
                                                                                'time', verbose=False)
        concatenated_time_data = concatenated_data.variables[concatenated_time_variable_name][...]
        try:
            concatenated_time_valid_min = concatenated_data.variables[concatenated_time_variable_name].valid_min
            concatenated_time_valid_max = concatenated_data.variables[concatenated_time_variable_name].valid_max
        except AttributeError:
            concatenated_time_valid_min = None
            concatenated_time_valid_max = None
        concatenated_time_reference = concatenated_data.variables[concatenated_time_variable_name].units
        concatenated_data.close()
        if in_records_number < minimum_records_threshold:
            time.sleep(sleep_time)
            print(' Warning:' + print_prefix + ' Too few data in selected period.', file=sys.stderr)
            time.sleep(sleep_time)
            print(print_prefix + ' -------------------------')
            processing_message = 'Too few data for period ' + str(first_date_str) + ' ' \
                                 + str(last_date_str)
            out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
            out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
            np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',', comments='')
            continue
        else:
            print(print_prefix + ' record coordinate size: ' + str(in_records_number) + ' records.')
        # Check if time valid_min attribute is not the opposite of valid_max, if it is correct it the the opposite
        if concatenated_time_valid_min is None or concatenated_time_valid_max is None:
            pass
        elif concatenated_time_valid_min != -concatenated_time_valid_max:
            time.sleep(sleep_time)
            print(' Warning:' + print_prefix + ' time valid_min seems different from the opposite of valid_max.'
                  ' Correcting it accordingly.', file=sys.stderr)
            time.sleep(sleep_time)
            valid_min_time_file = work_dir + '/' + out_file_name + '_time_valid_min_edited.nc'
            shutil.copy2(concatenated_file, valid_min_time_file)
            valid_min_time_data = netCDF4.Dataset(valid_min_time_file, mode='r+')
            valid_min_time_variable_name = find_variable_name.find_variable_name(valid_min_time_file, 'standard_name',
                                                                                 'time', verbose=False)
            valid_min_time_data.variables[valid_min_time_variable_name].valid_min = -concatenated_time_valid_max
            concatenated_time_data = valid_min_time_data.variables[valid_min_time_variable_name][...]
            valid_min_time_data.close()
            concatenated_file = valid_min_time_file
        if 'days' in concatenated_time_reference:
            concatenated_time_data = np.round(concatenated_time_data * 86400.)
        elif 'seconds' in concatenated_time_reference:
            concatenated_time_data = np.round(concatenated_time_data)
        concatenated_time_reference =\
            concatenated_time_reference[concatenated_time_reference.find('since ') + len('since '):]
        concatenated_reference_data =\
            np.abs(calendar.timegm(time.strptime(concatenated_time_reference, '%Y-%m-%dT%H:%M:%SZ')))
        concatenated_time_data -= concatenated_reference_data
        if np.ma.is_masked(concatenated_time_data):
            start_time_seconds = np.sort(concatenated_time_data[np.invert(concatenated_time_data.mask)])[0]
            end_time_seconds = np.sort(concatenated_time_data[np.invert(concatenated_time_data.mask)])[-1]
        else:
            start_time_seconds = np.sort(concatenated_time_data)[0]
            end_time_seconds = np.sort(concatenated_time_data)[-1]
        start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(np.round(start_time_seconds)))
        print(print_prefix + ' start recording time: ' + start_time)
        end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(np.round(end_time_seconds)))
        print(print_prefix + ' end recording time: ' + end_time)
        sampling_time_seconds = time_calc.time_calc(concatenated_file, verbose=False)
        sampling_time_days = sampling_time_seconds // 86400
        sampling_time_modulus = sampling_time_seconds - sampling_time_days * 86400
        sampling_time = time.strftime('%H:%M:%S', time.gmtime(sampling_time_modulus))
        if sampling_time_seconds == 1:
            print(print_prefix + ' most representative sampling time: 1 second')
        elif sampling_time_seconds < 60:
            print(print_prefix + ' most representative sampling time: ' + sampling_time + ' seconds')
        elif sampling_time_seconds == 60:
            print(print_prefix + ' most representative sampling time: ' + sampling_time + ' minute')
        elif sampling_time_seconds < 3600:
            print(print_prefix + ' most representative sampling time: ' + sampling_time + ' minutes')
        elif sampling_time_seconds == 3600:
            print(print_prefix + ' most representative sampling time: ' + sampling_time + ' hour')
        elif sampling_time_seconds < 86400:
            print(print_prefix + ' most representative sampling time: ' + sampling_time + ' hours')
        elif sampling_time_seconds == 86400:
            print(print_prefix + ' most representative sampling time: 1 day')
        else:
            print(print_prefix + ' DAC qc file sampling time: ' +
                  str(sampling_time_days) + ' days and ' + sampling_time)

        out_standard_names = list()
        out_long_names = list()
        out_units = list()
        mean_longitudes = list()
        mean_latitudes = list()
        start_dates = list()
        end_dates = list()
        sampling_times = list()
        valid_depth_levels = list()
        quality_controls = list()
        notes_array = list()

        for field in range(len(true_fields_standard_names)):
            out_processing_line = np.array([[os.path.basename(in_file), platform_code,
                                             organization_name, platform_name, wmo, platform_type,
                                             str(latitude_mean), str(latitude_mean)]])
            variable_standard_name = true_fields_standard_names[field]
            out_standard_names.append(variable_standard_name)
            quality_controls.append('')
            notes_array.append('')
            mean_longitudes.append('')
            mean_latitudes.append('')
            start_dates.append('')
            end_dates.append('')
            sampling_times.append('')
            valid_depth_levels.append('')
            variable_title = true_fields_long_names[field]
            print(print_prefix + ' -------------------------')
            print(print_prefix + ' Selected recorded variable: ' + variable_standard_name + ' (' + variable_title + ')')
            print(print_prefix + ' -------------------------')
            variable_valid_depth_levels = list()
            out_field_file_name = out_file_name + '_' + variable_standard_name
            work_field_dir = work_dir + '/' + variable_standard_name + '/'
            if not os.path.exists(work_field_dir):
                print(print_prefix + ' Creating work ' + variable_standard_name + ' data folder.')
                os.makedirs(work_field_dir)
            extracted_file = work_field_dir + out_field_file_name + '_extracted.nc'
            print(print_prefix + ' Extracting ' + variable_standard_name + ' field.')
            insitu_tac_timeseries_extractor.insitu_tac_timeseries_extractor(concatenated_file, variable_standard_name,
                                                                            extracted_file, first_date_str,
                                                                            last_date_str, verbose=verbose)
            if not os.path.isfile(extracted_file):
                time.sleep(sleep_time)
                print(' Warning:' + print_prefix +
                      ' no data in the selected period for field ' + variable_standard_name + '.', file=sys.stderr)
                time.sleep(sleep_time)
                print(print_prefix + ' -------------------------')
                processing_message = 'No data for period ' + str(first_date_str) + ' '\
                                     + str(last_date_str) + ' for field ' + variable_standard_name
                out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
                out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
                np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',',
                           comments='')
                variable_valid_depth_levels = list()
                del out_standard_names[-1]
                del quality_controls[-1]
                del notes_array[-1]
                del mean_longitudes[-1]
                del mean_latitudes[-1]
                del start_dates[-1]
                del end_dates[-1]
                del sampling_times[-1]
                del valid_depth_levels[-1]
                continue
            extracted_data = netCDF4.Dataset(extracted_file, mode='r')
            out_depth_dimension = len(extracted_data.dimensions['depth'])
            try:
                variable_long_name = extracted_data.variables[variable_standard_name].long_name
            except AttributeError:
                variable_long_name = ''
            try:
                variable_units = extracted_data.variables[variable_standard_name].units
            except AttributeError:
                variable_units = ''
            extracted_data.close()
            out_long_names.append(variable_long_name)
            out_units.append(variable_units)
            no_quality_controls = True
            full_quality_controls = True
            for depth in range(out_depth_dimension):
                [total_records_number, no_qc_values_number, no_qc_last_index, valid_values_number, valid_last_index,
                    filled_values_number, filled_last_index, invalid_values_number, invalid_last_index] =\
                    data_information_calc.data_information_calc(extracted_file, variable_standard_name,
                                                                valid_qc_values, depth,
                                                                first_date_str, last_date_str, verbose=False)

                no_qc_values_percentage = np.round(no_qc_values_number / total_records_number * 100)
                valid_values_percentage = np.round(valid_values_number / total_records_number * 100)
                filled_values_percentage = np.round(filled_values_number / total_records_number * 100)
                invalid_values_percentage = np.round(invalid_values_number / total_records_number * 100)
                if (valid_last_index > -1) or (no_qc_last_index > -1):
                    variable_valid_depth_levels.append(str(depth + 1))
                else:
                    continue
                if valid_last_index > -1:
                    variable_qc_last_record_seconds = time_from_index.time_from_index(extracted_file,
                                                                                      valid_last_index, verbose=False)
                    variable_qc_last_record_time =\
                        time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(variable_qc_last_record_seconds))
                if no_qc_last_index > -1:
                    variable_no_qc_last_record_seconds = time_from_index.time_from_index(extracted_file,
                                                                                         no_qc_last_index,
                                                                                         verbose=False)
                    variable_no_qc_last_record_time =\
                        time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(variable_no_qc_last_record_seconds))
                if (valid_last_index > -1) or (no_qc_last_index > -1):
                    print(print_prefix + ' depth level ' + str(depth + 1) + ':')
                    if (valid_values_number == 0) and (invalid_values_number == 0):
                        time.sleep(sleep_time)
                        print(' Warning:' + print_prefix + ' no quality control for field ' + variable_standard_name +
                              ' at depth level ' + str(depth + 1) + '.', file=sys.stderr)
                        time.sleep(sleep_time)
                        print(print_prefix + ' not quality checked data: '
                              + str(no_qc_values_number) + ' (' + str(no_qc_values_percentage) + '%)')
                        print(print_prefix + ' missing data: '
                              + str(filled_values_number) + ' (' + str(filled_values_percentage) + '%)')
                        print(print_prefix + ' last no qc data recorded time: '
                              + str(variable_no_qc_last_record_time))
                        full_quality_controls = False
                    elif (valid_values_number == 0) and (invalid_values_number > 0) and (no_qc_values_number > 0):
                        time.sleep(sleep_time)
                        print(' Warning:' + print_prefix +
                              ' good data seems to be flagged by 0 and not by 1 for field ' + variable_standard_name +
                              ' at depth level ' + str(depth + 1) + '.', file=sys.stderr)
                        time.sleep(sleep_time)
                        print(print_prefix + ' not quality checked data: '
                              + str(no_qc_values_number) + ' (' + str(no_qc_values_percentage) + '%)')
                        print(print_prefix + ' bad data: '
                              + str(invalid_values_number) + ' (' + str(invalid_values_percentage) + '%)')
                        print(print_prefix + ' missing data: '
                              + str(filled_values_number) + ' (' + str(filled_values_percentage) + '%)')
                        print(print_prefix + ' last valid data recorded time: '
                              + str(variable_no_qc_last_record_time))
                    elif no_qc_values_number > 0:
                        time.sleep(sleep_time)
                        print(' Warning:' + print_prefix + ' data partially stored without quality control for field '
                              + variable_standard_name + ' at depth level ' + str(depth + 1) + '.', file=sys.stderr)
                        time.sleep(sleep_time)
                        print(print_prefix + ' quality checked data: '
                              + str(valid_values_number) + ' (' + str(valid_values_percentage) + '%)')
                        print(print_prefix + ' not quality checked data: '
                              + str(no_qc_values_number) + ' (' + str(no_qc_values_percentage) + '%)')
                        print(print_prefix + ' bad data: '
                              + str(invalid_values_number) + ' (' + str(invalid_values_percentage) + '%)')
                        print(print_prefix + ' missing data: '
                              + str(filled_values_number) + ' (' + str(filled_values_percentage) + '%)')
                        print(print_prefix + ' last valid data recorded time: '
                              + str(variable_qc_last_record_time))
                        print(print_prefix + ' last no qc data recorded time: '
                              + str(variable_no_qc_last_record_time))
                        no_quality_controls = False
                        full_quality_controls = False
                    else:
                        print(print_prefix + ' quality checked data: '
                              + str(valid_values_number) + ' (' + str(valid_values_percentage) + '%)')
                        print(print_prefix + ' bad data: '
                              + str(invalid_values_number) + ' (' + str(invalid_values_percentage) + '%)')
                        print(print_prefix + ' missing data: '
                              + str(filled_values_number) + ' (' + str(filled_values_percentage) + '%)')
                        print(print_prefix + ' last valid data recorded time: '
                              + str(variable_qc_last_record_time))
                        no_quality_controls = False
                    print(print_prefix + ' -------------------------')
            if full_quality_controls and no_quality_controls:
                quality_controls[-1] = 'INVERTED'
            elif full_quality_controls and not no_quality_controls:
                quality_controls[-1] = 'FULL'
            elif not full_quality_controls and no_quality_controls:
                quality_controls[-1] = 'NO'
            else:
                quality_controls[-1] = 'PARTIAL'

            dac_qc_file = work_field_dir + out_field_file_name + '_dac_qc.nc'
            print(print_prefix + ' Applying DAC QC to ' + variable_standard_name + ' field.')
            quality_check_applier.quality_check_applier(extracted_file, variable_standard_name, valid_qc_values,
                                                        dac_qc_file, iteration=-1, verbose=verbose)
            if not os.path.isfile(dac_qc_file):
                time.sleep(sleep_time)
                print(' Warning:' + print_prefix + ' no valid data in the selected period for field '
                      + variable_standard_name + '.', file=sys.stderr)
                time.sleep(sleep_time)
                print(print_prefix + ' -------------------------')
                processing_message = 'No valid data for period ' + str(first_date_str) + ' '\
                                     + str(last_date_str) + ' for field ' + variable_standard_name
                out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
                out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
                np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',',
                           comments='')
                variable_valid_depth_levels = list()
                del out_standard_names[-1]
                del out_long_names[-1]
                del out_units[-1]
                del quality_controls[-1]
                del notes_array[-1]
                del mean_longitudes[-1]
                del mean_latitudes[-1]
                del start_dates[-1]
                del end_dates[-1]
                del sampling_times[-1]
                del valid_depth_levels[-1]
                continue
            dac_qc_data = netCDF4.Dataset(dac_qc_file, mode='r+')
            dac_qc_data.wmo_platform_code = wmo
            try:
                dac_qc_data.edmo_code = edmo_code
            except TypeError:
                pass
            dac_qc_data.SOURCE_platform_code = out_platform_code
            dac_qc_data.platform_name = out_platform_name
            dac_qc_time = dac_qc_data.variables['time']
            dac_qc_time_reference = dac_qc_time.units
            dac_qc_time_reference_str = dac_qc_time_reference[dac_qc_time_reference.find('since ') + len('since '):]
            dac_qc_reference_data = abs(calendar.timegm(time.strptime(dac_qc_time_reference_str, '%Y-%m-%dT%H:%M:%SZ')))
            dac_qc_time_data = dac_qc_data.variables['time'][...] - dac_qc_reference_data
            if np.ma.is_masked(dac_qc_time_data):
                dac_qc_time_data = dac_qc_time_data[np.invert(dac_qc_time_data.mask)]
            unique_time_records = len(np.unique(dac_qc_time_data))
            start_date_seconds = np.min(dac_qc_time_data)
            start_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(start_date_seconds))
            start_dates[-1] = str(start_date)
            end_date_seconds = np.max(dac_qc_time_data)
            end_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(end_date_seconds))
            end_dates[-1] = str(end_date)
            dac_qc_data.close()
            if (len(variable_valid_depth_levels) == 0) or (unique_time_records < minimum_records_threshold):
                time.sleep(sleep_time)
                print(' Warning:' + print_prefix + ' Too few or no valid data in selected period for field '
                      + variable_standard_name + '.', file=sys.stderr)
                time.sleep(sleep_time)
                print(print_prefix + ' -------------------------')
                processing_message = 'Too few or no valid data for period ' + str(first_date_str) + ' '\
                                     + str(last_date_str) + ' for field ' + variable_standard_name
                out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
                out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
                np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',',
                           comments='')
                variable_valid_depth_levels = list()
                del out_standard_names[-1]
                del out_long_names[-1]
                del out_units[-1]
                del quality_controls[-1]
                del notes_array[-1]
                del mean_longitudes[-1]
                del mean_latitudes[-1]
                del start_dates[-1]
                del end_dates[-1]
                del sampling_times[-1]
                del valid_depth_levels[-1]
                continue
            else:
                print(print_prefix + ' valid depth levels for this variable: ' + ' '.join(variable_valid_depth_levels))
                print(print_prefix + ' analyzing variable ' + variable_standard_name + '...')
                print(print_prefix + ' checking time step.')
                time_step_check = time_check.time_check(dac_qc_file, verbose=False)
                if time_step_check == 0:
                    print(print_prefix +
                          ' Time monotonically increases without duplicates'
                          ' between one time step and another.')
                elif time_step_check == 1:
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix +
                          ' duplicated time records for field ' + variable_standard_name + '.', file=sys.stderr)
                    time.sleep(sleep_time)
                    notes_array[-1] = 'duplicated records '
                elif time_step_check == 2:
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix +
                          ' wrong positioning records for field ' + variable_standard_name + '.', file=sys.stderr)
                    time.sleep(sleep_time)
                    notes_array[-1] = 'reversed records '
                elif time_step_check == 3:
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix +
                          ' duplicated entries and wrong positioning records for field '
                          + variable_standard_name + '.', file=sys.stderr)
                    time.sleep(sleep_time)
                    notes_array[-1] = 'duplicated and reversed records '
                [dac_qc_longitude_mean, dac_qc_longitude_variance] =\
                    mean_variance_nc_variable.mean_variance_nc_variable(dac_qc_file, 'longitude', verbose=False)
                if np.ma.is_masked(dac_qc_longitude_mean):
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix + ' not valid sensor average longitude for field '
                          + variable_standard_name + '.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    processing_message = 'Not valid sensor average longitude for field ' + variable_standard_name
                    out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
                    out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
                    np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',',
                               comments='')
                    variable_valid_depth_levels = list()
                    del out_standard_names[-1]
                    del out_long_names[-1]
                    del out_units[-1]
                    del quality_controls[-1]
                    del notes_array[-1]
                    del mean_longitudes[-1]
                    del mean_latitudes[-1]
                    del start_dates[-1]
                    del end_dates[-1]
                    del sampling_times[-1]
                    del valid_depth_levels[-1]
                    continue
                if dac_qc_longitude_mean > 180:
                    dac_qc_longitude_mean -= 360
                dac_qc_longitude_unique_values =\
                    unique_values_nc_variable.unique_values_nc_variable(dac_qc_file, 'longitude', verbose=False)
                print(print_prefix + ' mean sensor longitude : ' + str(dac_qc_longitude_mean) + ' degrees west')
                print(print_prefix + ' sensor longitude variance : ' + str(dac_qc_longitude_variance) + ' degrees')
                mean_longitudes[-1] = str(np.around(dac_qc_longitude_mean, decimals=3))
                [dac_qc_latitude_mean, dac_qc_latitude_variance] =\
                    mean_variance_nc_variable.mean_variance_nc_variable(dac_qc_file, 'latitude', verbose=False)
                if np.ma.is_masked(dac_qc_latitude_mean):
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix + ' not valid sensor average latitude for field '
                          + variable_standard_name + '.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    processing_message = 'Not valid sensor average latitude for field ' + variable_standard_name
                    out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
                    out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
                    np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',',
                               comments='')
                    variable_valid_depth_levels = list()
                    del out_standard_names[-1]
                    del out_long_names[-1]
                    del out_units[-1]
                    del quality_controls[-1]
                    del notes_array[-1]
                    del mean_longitudes[-1]
                    del mean_latitudes[-1]
                    del start_dates[-1]
                    del end_dates[-1]
                    del sampling_times[-1]
                    del valid_depth_levels[-1]
                    continue
                if dac_qc_latitude_mean > 90:
                    dac_qc_latitude_mean -= 180
                dac_qc_latitude_unique_values = \
                    unique_values_nc_variable.unique_values_nc_variable(dac_qc_file, 'latitude', verbose=False)
                print(print_prefix + ' mean sensor latitude : ' + str(dac_qc_latitude_mean) + ' degrees east')
                print(print_prefix + ' sensor latitude variance : ' + str(dac_qc_latitude_variance) + ' degrees')
                mean_latitudes[-1] = str(np.around(dac_qc_latitude_mean, decimals=3))
                if (dac_qc_longitude_variance < variance_duplicate_threshold) or \
                        (dac_qc_longitude_unique_values < total_records_number / 2) or \
                        (dac_qc_latitude_variance < variance_duplicate_threshold) or \
                        (dac_qc_latitude_unique_values < total_records_number / 2):
                    [depth_information_array, out_depth_levels] = \
                        depth_calc.depth_calc(dac_qc_file, variable_standard_name, verbose=False)
                    print(print_prefix + ' computing new depth data.')
                    out_depth_levels_list = list(map(str, out_depth_levels))
                    if out_depth_levels.shape[0] == 0:
                        time.sleep(sleep_time)
                        print(' Warning:' + print_prefix +
                              ' no valid data for this variable in selected period for field '
                              + variable_standard_name + '.', file=sys.stderr)
                        time.sleep(sleep_time)
                        print(print_prefix + ' -------------------------')
                        processing_message = 'No valid depth levels for field ' + variable_standard_name
                        out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
                        out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
                        np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',',
                                   comments='')
                        variable_valid_depth_levels = list()
                        del out_standard_names[-1]
                        del out_long_names[-1]
                        del out_units[-1]
                        del quality_controls[-1]
                        del notes_array[-1]
                        del mean_longitudes[-1]
                        del mean_latitudes[-1]
                        del start_dates[-1]
                        del end_dates[-1]
                        del sampling_times[-1]
                        del valid_depth_levels[-1]
                        continue
                    print(print_prefix + ' Output depths for this variable: '
                          + ' '.join(out_depth_levels_list) + ' meters.')
                    [depth_is_constant, depth_is_good_spaced, good_data_depth_levels,
                     depth_is_positive] = depth_information_array
                    valid_depth_levels[-1] = ' '.join(out_depth_levels_list)
                    if not depth_is_constant and depth_is_good_spaced:
                        notes_array[-1] += ' probably replaced sensors'
                    elif not depth_is_constant and not depth_is_good_spaced:
                        notes_array[-1] += ' not averaged depth levels'
                    # elif notes_array[-1] == '':
                    #     notes_array[-1] += 'none'
                else:
                    valid_depth_levels[-1] = 'floating'
                dac_qc_time_delta = datetime.timedelta(seconds=end_date_seconds - start_date_seconds)
                if (not update_mode) and (dac_qc_time_delta < datetime.timedelta(days=minimum_record_days_threshold)):
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix +
                          ' DAC qc file record segment is below ' + str(minimum_record_days_threshold) +
                          ' days for field ' + variable_standard_name + '.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    processing_message = 'Data quantity under ' + str(minimum_record_days_threshold) + \
                                         ' days for field ' + variable_standard_name
                    out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
                    out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
                    np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',', comments='')
                    variable_valid_depth_levels = list()
                    del out_standard_names[-1]
                    del out_long_names[-1]
                    del out_units[-1]
                    del quality_controls[-1]
                    del notes_array[-1]
                    del mean_longitudes[-1]
                    del mean_latitudes[-1]
                    del start_dates[-1]
                    del end_dates[-1]
                    del valid_depth_levels[-1]
                    continue
                dac_qc_sampling_time_seconds = time_calc.time_calc(dac_qc_file, verbose=False)
                dac_qc_sampling_time_days = dac_qc_sampling_time_seconds // 86400
                dac_qc_sampling_time_modulus = dac_qc_sampling_time_seconds - dac_qc_sampling_time_days * 86400
                dac_qc_sampling_time_hours = time.strftime('%H:%M:%S', time.gmtime(dac_qc_sampling_time_modulus))
                if dac_qc_sampling_time_days < 9:
                    dac_qc_sampling_time = '00' + str(dac_qc_sampling_time_days) + ' ' + dac_qc_sampling_time_hours
                elif dac_qc_sampling_time_days < 99:
                    dac_qc_sampling_time = '0' + str(dac_qc_sampling_time_days) + ' ' + dac_qc_sampling_time_hours
                elif dac_qc_sampling_time_days < 999:
                    dac_qc_sampling_time = str(dac_qc_sampling_time_days) + ' ' + dac_qc_sampling_time_hours
                else:
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix +
                          ' DAC qc file sampling time is above 999 days (not handled at the moment) for field '
                          + variable_standard_name + '.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    processing_message = 'Sampling time above 999 days for field ' + variable_standard_name
                    out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
                    out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
                    np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',',
                               comments='')
                    variable_valid_depth_levels = list()
                    del out_standard_names[-1]
                    del out_long_names[-1]
                    del out_units[-1]
                    del quality_controls[-1]
                    del notes_array[-1]
                    del mean_longitudes[-1]
                    del mean_latitudes[-1]
                    del start_dates[-1]
                    del end_dates[-1]
                    del valid_depth_levels[-1]
                    continue
                if dac_qc_sampling_time_seconds == 1:  # 1 second
                    print(print_prefix + ' DAC qc file sampling time: 1 second')
                elif dac_qc_sampling_time_seconds < 60:  # 1 minute
                    print(print_prefix + ' DAC qc file sampling time: ' + dac_qc_sampling_time + ' seconds')
                elif dac_qc_sampling_time_seconds == 60:  # 1 minute
                    print(print_prefix + ' DAC qc file sampling time: 1 minute')
                elif dac_qc_sampling_time_seconds < 3600:  # 1 hour
                    print(print_prefix + ' DAC qc file sampling time: ' + dac_qc_sampling_time + ' minutes')
                elif dac_qc_sampling_time_seconds == 3600:  # 1 hour
                    print(print_prefix + ' DAC qc file sampling time: 1 hour')
                elif dac_qc_sampling_time_seconds < 86400:  # 1 day
                    print(print_prefix + ' DAC qc file sampling time: ' + dac_qc_sampling_time + ' hours')
                elif dac_qc_sampling_time_seconds == 86400:  # 1 day
                    print(print_prefix + ' DAC qc file sampling time: 1 day')
                elif dac_qc_sampling_time_days < 999:  # maximum handled days
                    print(print_prefix + ' DAC qc file sampling time: ' + dac_qc_sampling_time + ' days')

                sampling_times[-1] = dac_qc_sampling_time
            # break  # to produce only the first selected field of every archive.

        if len(out_standard_names) > 0:
            if out_platform_type not in out_devices_data[:, 1]:
                device_id += 1
                out_devices_line = np.array([[device_id, out_platform_type]], dtype=object)
                out_devices_data = np.append(out_devices_data, out_devices_line, axis=0)
                print(print_prefix + ' Writing output devices CSV file...')
                np.savetxt(out_devices_file, out_devices_data, fmt='"%s"', delimiter=',', comments='')
            device_ids = out_devices_data[:, 0]
            device_names = out_devices_data[:, 1]
            device_index = np.where(device_names == out_platform_type)[0][0]
            out_device_id = device_ids[device_index]

            if out_organization_name not in out_organizations_data[:, 1]:
                organization_id += 1
                out_organizations_line = \
                    np.array([[organization_id, out_organization_name, organization_country, organization_link]],
                             dtype=object)
                out_organizations_data = np.append(out_organizations_data, out_organizations_line, axis=0)
                print(print_prefix + ' Writing output organizations CSV file...')
                np.savetxt(out_organizations_file, out_organizations_data,
                           fmt='"%s"', delimiter=',', comments='')
            organization_ids = out_organizations_data[:, 0]
            organization_names = out_organizations_data[:, 1]
            organization_index = np.where(organization_names == out_organization_name)[0][0]
            out_organization_id = organization_ids[organization_index]

            probe_id += 1
            variable_ids = []
            for standard_name_index in range(len(out_standard_names)):
                variable_standard_name = out_standard_names[standard_name_index]
                variable_index = in_fields_standard_name_list.index(variable_standard_name)
                variable_ids.append(str(in_variable_ids[variable_index]))
                out_variable_ids = out_variables_data[:, 0]
                if in_variable_ids[variable_index] not in out_variable_ids:
                    variable_long_name = out_long_names[standard_name_index]
                    variable_units = out_units[standard_name_index]
                    out_variables_line = np.array(
                        [[in_variable_ids[variable_index], variable_standard_name,
                          variable_long_name, variable_units]], dtype=object)
                    out_variables_data = np.append(out_variables_data, out_variables_line, axis=0)
                    print(print_prefix + ' Writing output variables CSV file...')
                    np.savetxt(out_variables_file, out_variables_data, fmt='"%s"', delimiter=',',
                               comments='')

            if csv_platform_name is not None:
                out_platform_name = csv_platform_name
            elif (out_platform_name == '') or (out_platform_name == ' '):
                out_platform_name = out_platform_code
            out_probes_line = \
                np.array([[probe_id, out_platform_code, out_platform_name, out_wmo, out_device_id, out_organization_id,
                           ';'.join(variable_ids),
                           ';'.join(mean_longitudes), ';'.join(mean_latitudes),
                           ';'.join(start_dates), ';'.join(end_dates),
                           ';'.join(sampling_times), ';'.join(valid_depth_levels),
                           ';'.join(quality_controls), ';'.join(notes_array), out_probe_link]], dtype=object)
            # out_probes_line = \
            #     np.array([[probe_id, out_platform_code, out_platform_name, out_wmo, out_platform_type,
            #                out_organization_name, ';'.join(out_standard_names),
            #                ';'.join(mean_longitudes), ';'.join(mean_latitudes),
            #                ';'.join(start_dates), ';'.join(end_dates),
            #                ';'.join(sampling_times), ';'.join(valid_depth_levels),
            #                ';'.join(quality_controls), ';'.join(notes_array), out_probe_link]], dtype=object)

            out_probes_data = np.append(out_probes_data, out_probes_line, axis=0)
            print(print_prefix + ' Writing output probes CSV file...')
            np.savetxt(out_probes_file, out_probes_data, fmt='"%s"', delimiter=',', comments='')

            if processing_message is None:
                processing_message = 'OK'
                out_processing_line = np.append(out_processing_line, np.array([[processing_message]]), axis=1)
                out_processing_data = np.append(out_processing_data, out_processing_line, axis=0)
                np.savetxt(out_processing_file, out_processing_data, fmt='"%s"', delimiter=',', comments='')

        for variable_standard_name in out_standard_names:
            work_field_dir = work_dir + '/' + variable_standard_name + '/'
            out_field_file_name = out_file_name + '_' + variable_standard_name
            dac_qc_file = work_field_dir + out_field_file_name + '_dac_qc.nc'
            if not os.path.isfile(dac_qc_file):
                continue
            out_field_dir = out_dir + '/' + variable_standard_name + '/'
            if not os.path.exists(out_field_dir):
                print(' Creating output ' + variable_standard_name + ' data folder.')
                os.makedirs(out_field_dir)
            out_file = out_field_dir + out_field_file_name + '_dac_qc.nc'
            print(print_prefix + ' copying ' + variable_standard_name + ' extracted file to output directory.')
            shutil.copy2(dac_qc_file, out_file)

        time_diff = time.gmtime(calendar.timegm(time.gmtime()) - run_time)
        print(print_prefix + ' -------------------------')
        print(print_prefix + ' input file completed. ETA is ' + time.strftime('%H:%M:%S', time_diff))
        print(print_prefix + ' -------------------------')

        # break  # to post process only the first archive in the list

    print(' -------------------------')
    total_run_time = time.gmtime(calendar.timegm(time.gmtime()) - start_run_time)
    print(' Finished! Total elapsed time is: '
          + str(int(np.floor(calendar.timegm(total_run_time) / 86400.))) + ' days '
          + time.strftime('%H:%M:%S', total_run_time) + ' hh:mm:ss')


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_dir = sys.argv[1]
        in_fields_standard_name_str = sys.argv[2]
        work_dir = sys.argv[3]
        out_dir = sys.argv[4]
        valid_qc_values = sys.argv[5]
    except (IndexError, ValueError):
        in_dir = None
        in_fields_standard_name_str = None
        work_dir = None
        out_dir = None
        valid_qc_values = None

    try:
        update_mode = string_to_bool(sys.argv[6])
    except (IndexError, ValueError):
        update_mode = False

    try:
        first_date_str = sys.argv[7]
    except (IndexError, ValueError):
        first_date_str = None

    try:
        last_date_str = sys.argv[8]
    except (IndexError, ValueError):
        last_date_str = None

    try:
        region_boundaries_str = sys.argv[9]
    except (IndexError, ValueError):
        region_boundaries_str = None

    try:
        med_sea_masking = string_to_bool(sys.argv[10])
    except (IndexError, ValueError):
        med_sea_masking = False

    try:
        in_instrument_types_str = sys.argv[11]
    except (IndexError, ValueError):
        in_instrument_types_str = None

    try:
        names_file = sys.argv[12]
    except (IndexError, ValueError):
        names_file = None

    try:
        verbose = string_to_bool(sys.argv[13])
    except (IndexError, ValueError):
        verbose = True

    insitu_tac_pre_processing(in_dir, in_fields_standard_name_str, work_dir, out_dir, valid_qc_values,
                              update_mode, first_date_str, last_date_str, region_boundaries_str, med_sea_masking,
                              in_instrument_types_str, names_file, verbose)
