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
from SOURCE.obs_postpro import time_averager, time_series_post_processing, \
    quality_check_applier, depth_aggregator, depth_calc
from SOURCE import duplicated_records_remover, records_monotonicity_fixer, time_check, time_calc

# Global variables
sleep_time = 0.1  # seconds
time_step_tolerance = 10  # percent, to accept datasets input sampling slightly greater than selected output sampling

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

# Global range check
global_range_check_enabled = True
# Spike test
spike_test_enabled = True
# Stuck value_test
stuck_value_test_enabled = True


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


def obs_postpro(in_csv_dir=None, in_dir=None, in_fields_standard_name_str=None, work_dir=None, out_dir=None,
                routine_qc_iterations=None, climatology_dir=None, first_date_str=None, last_date_str=None,
                region_boundaries_str=None, med_sea_masking=False, in_instrument_types_str=None, verbose=True):
    """
    Script to post process in situ devices datasets
    from an already downloaded database with optional real time execution CSV table needed.

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

        2) Pre processed in situ observations datasets directory, divided by standard_name;

        3) Input variables standard_name attributes to process space separated string
            (for example: "sea_water_temperature sea_water_practical_salinity", please read CF conventions standard name
            table to find the correct strings to insert);

        4) Base working directory;

        5) Output post processed datasets directory;

        6) Routine QC iterations to compute (integer):
            a) -1 for original quality controls only (QC);
            b) 0 for gross check quality controls only (NO_SPIKES_QC);
            c) N >= 1 for N statistic quality check iterations;

        7) Climatology data directory, divided by standard_name (OPTIONAL);

        8) Start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format (OPTIONAL);

        9) End date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS (OPTIONAL);

        10) Region longitude - latitude limits space separated string
            (min_lon, max_lon (deg E); min_lat, max_lat (deg N), OPTIONAL);

        11) Masking foreign seas switch for Mediterranean Sea processing (OPTIONAL);

        12) Input CMEMS "instrument type" metadata filter space separated string
            (for example: "\"mooring\" \"coastal structure\"", please read CMEMS manual to properly write
            the attribute string, PLEASE put attributes with spaces with quotes to protect them
            from character escaping);

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
            p) Probe weblink (if available).

        5) If routine_qc_iterations is greater or equal than 0, rejection data table, with the sequent header:
            a) Probe CMEMS platform_code attribute;
            b) Variable standard_names;
            c) Total data amount for each variable;
            d) filled data for each variable;
            e) rejection amount for each variable by global range check data;
            f) rejection amount for each variable by spike test data;
            g) rejection amount for each variable by stuck value test data;
            h) (if routine_qc_iterations is greater or equal than 1)
                    rejection amount for each variable for each statistic phase..

        6) Post processed in situ files in netCDF-4 format, divided by hourly and daily means and probe field,
                containing:

            a) probe latitude;
            b) probe longitude;
            c) field depths;
            d) time counter and boundaries;
            e) RAW, post processed and averaged fields;
            f) global attributes containing original datasets and post process specs.

        7) Per-probe and per-field monthly mean climatology averages, standard deviation and filtered density
            profiles dataset.

    Written Oct 16, 2017 by Paolo Oliveri
    """
    if __name__ == '__main__':
        return
    start_run_time = calendar.timegm(time.gmtime())
    # Main Program
    print('-------------------------' + ' ' + __file__ + ' -------------------------')
    print(' Local time is ' + time.strftime("%a %b %d %H:%M:%S %Z %Y", time.gmtime()))
    print(' -------------------------')
    print(' Post processing in situ probes pre processed observation files.')
    print(' -------------------------')
    if in_csv_dir is None or in_dir is None or in_fields_standard_name_str is None or work_dir is None or\
            out_dir is None or routine_qc_iterations is None:
        time.sleep(sleep_time)
        print(' ERROR: 6 of 13 maximum arguments (7 optionals) not provided.', file=sys.stderr)
        print(' 1) Pre processed in situ information CSV directory;', file=sys.stderr)
        print(' 2) Pre processed observations netCDF database directory;', file=sys.stderr)
        print(' 3) Input fields standard_name space separated string to process'
              ' (for example: "sea_water_temperature sea_water_practical_salinity");', file=sys.stderr)
        print(' 4) Working directory;', file=sys.stderr)
        print(' 5) Output directory;', file=sys.stderr)
        print(' 6) Routine quality check iterations number (N, integer), options:', file=sys.stderr)
        print('     a) N = -1 for original DAC quality controls only (NO QC);', file=sys.stderr)
        print('     b) N = 0 for gross check quality controls only (GROSS_QC);', file=sys.stderr)
        print('     c) N >= 1 for N statistic quality check iterations (STATISTIC_QC_N);', file=sys.stderr)
        print(' 7) (optional) Platform climatology data directory;', file=sys.stderr)
        print(' 8) (optional) First date to evaluate in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: first recorded date for each device);', file=sys.stderr)
        print(' 9) (optional) Last date to evaluate in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: last recorded date for each device);', file=sys.stderr)
        print(' 10) (optional) Region longitude - latitude limits space separated string'
              ' (min_lon, max_lon (deg E); min_lat, max_lat (deg N), default: "-180 180 0 180" (all the Earth))',
              file=sys.stderr)
        print(' 11) (optional) Masking foreign seas switch for Mediterranean Sea processing switch (True or False)'
              ' (default: False).', file=sys.stderr)
        print(' 12) (optional) Input "instrument type" metadata filter (space separated string, '
              ' for example: \'"mooring" "coastal structure"\');', file=sys.stderr)
        print(' 13) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return

    if (region_boundaries_str is None) or (region_boundaries_str == 'None') or (region_boundaries_str == ''):
        region_boundaries_str = '-180 180 -90 90'

    # Box boundaries to process devices
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

    print(' Pre processed in situ information CSV directory = ' + in_csv_dir)
    print(' Pre processed directory = ' + in_dir)
    print(' Input variables to process standard_name string = ' + in_fields_standard_name_str)
    print(' Working directory = ' + work_dir)
    print(' Output directory = ' + out_dir)
    print(' Statistic quality check iterations number = ' + str(routine_qc_iterations))
    print(' Climatology data directory = ' + str(climatology_dir) +
          ' (if None statistics will be computed from data itself)')
    print(' First date to process = ' + str(first_date_str) +
          ' (if None it will be the first available date on each device)')
    print(' Last date to process = ' + str(last_date_str) +
          ' (if None it will be the last available date on each device)')
    print(' Region boundary horizontal limits (min_lon, max_lon (deg E); min_lat, max_lat (deg N)) = '
          + region_boundaries_str)
    print(' Masking foreign seas switch for Med Sea processing switch = ' + str(med_sea_masking))
    print(' Input "instrument / type" metadata filter string = ' + str(in_instrument_types_str) +
          ' (if None all instruments will be processed)')
    print(' verbosity switch = ' + str(verbose))
    print(' -------------------------')
    print(' Starting process...')
    print(' -------------------------')

    record_folders = ['hm', 'dm', 'dm_shift', 'mm', 'ym']

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
    organization_countries = in_organizations_data[:, 2]
    organization_links = in_organizations_data[:, 3]

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
    variable_units = in_variables_data[:, 3]

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
    probes_longitudes = [longitudes.split(';') for longitudes in in_probes_data[:, 7]]
    probes_latitudes = [latitudes.split(';') for latitudes in in_probes_data[:, 8]]
    probes_record_starts = [record_starts.split(';') for record_starts in in_probes_data[:, 9]]
    probes_record_ends = [record_ends.split(';') for record_ends in in_probes_data[:, 10]]
    probes_sampling_times = [sampling_times.split(';') for sampling_times in in_probes_data[:, 11]]
    probes_depths = \
        [[depths.split(' ') for depths in split_depths.split(';')] for split_depths in in_probes_data[:, 12]]
    probes_quality_controls = [quality_controls.split(';') for quality_controls in in_probes_data[:, 13]]
    probes_notes = [notes.split(';') for notes in in_probes_data[:, 14]]

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

    if (climatology_dir == 'None') or (climatology_dir == ''):
        climatology_dir = None

    if climatology_dir:
        if not os.path.exists(climatology_dir):
            time.sleep(sleep_time)
            print(' Error. Climatology directory not found.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return
        update_mode = True
    else:
        update_mode = False

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

    if routine_qc_iterations >= 0:
        out_rejection_file = out_dir + '/rejection_process.csv'
        out_rejection_data = np.array([['platform_code', 'standard_names', 'data_total', 'filled_data']], dtype=object)
        if global_range_check_enabled:
            out_rejection_data = np.append(out_rejection_data, np.array([['global_range_check_rejection']]), axis=1)
        if spike_test_enabled:
            out_rejection_data = np.append(out_rejection_data, np.array([['spike_test_rejection']]), axis=1)
        if stuck_value_test_enabled:
            out_rejection_data = np.append(out_rejection_data, np.array([['stuck_value_rejection']]), axis=1)
        if routine_qc_iterations >= 1:
            for iteration in range(1, routine_qc_iterations + 1):
                out_rejection_data = \
                    np.append(out_rejection_data, np.array([['statistic_rejection_' + str(iteration)]]), axis=1)

        print(' Writing output probes CSV file header...')
        np.savetxt(out_rejection_file, out_rejection_data, fmt='"%s"', delimiter=',', comments='')

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
    device_id = 0
    organization_id = 0
    probe_id = 0
    in_variable_ids = np.arange(1, len(in_fields_standard_name_list) + 1)
    for in_csv_row in range(len(probes_platform_codes)):
        platform_code = probes_platform_codes[in_csv_row]
        completion_percentage = \
            np.around(len(progression_percentage_list) / len(probes_platform_codes) * 100, decimals=1)
        progression_percentage_list.append(platform_code)
        if platform_code in out_probes_data[:, 1]:
            time.sleep(sleep_time)
            print(' Warning:' + print_prefix + ' duplicated platform_code in input probes table. Skipping...',
                  file=sys.stderr)
            time.sleep(sleep_time)
            print(print_prefix + ' -------------------------')
            continue
        # if platform_code != '':
        #     continue
        print_prefix = ' (' + platform_code + ')'
        if verbose:
            print(print_prefix + ' -------------------------')
            print(print_prefix + ' Local time is ' + time.strftime("%Y-%m-%d %H:%M:%S %Z", time.gmtime()))
            print(print_prefix + ' Processing completion percentage: ' + str(completion_percentage) + '%')
        run_time = calendar.timegm(time.gmtime())
        print(print_prefix + ' platform code = \'' + platform_code + '\'')
        probe_name = probes_names[in_csv_row]
        print(print_prefix + ' probe name = \'' + probe_name + '\'')
        probe_wmo = probes_wmo[in_csv_row]
        print(print_prefix + ' WMO platform code = \'' + probe_wmo + '\'')
        probe_type = probes_types[in_csv_row]
        print(print_prefix + ' probe type = \'' + probe_type + '\'')
        probe_organization = probes_organizations[in_csv_row]
        print(print_prefix + ' probe organization: ' + probe_organization)
        organization_index = np.where(organization_names == probe_organization)[0][0]
        organization_country = organization_countries[organization_index]
        organization_link = organization_links[organization_index]
        probe_standard_names = probes_standard_names[in_csv_row]
        print(print_prefix + ' Pre processed ocean data :')
        if (in_instrument_types_list is not None) and (probe_type not in in_instrument_types_list):
            time.sleep(sleep_time)
            print(' Warning:' + print_prefix + ' in situ device type not in selected list.',
                  file=sys.stderr)
            time.sleep(sleep_time)
            print(print_prefix + ' -------------------------')
            continue
        for field_number in range(len(probe_standard_names)):
            recorded_variable = probe_standard_names[field_number]
            print(print_prefix + ' ' + str(field_number + 1) + ') ' + recorded_variable)
        intersection_standard_names = \
            [standard_name for standard_name in in_fields_standard_name_list if standard_name in probe_standard_names]
        if len(intersection_standard_names) == 0:
            time.sleep(sleep_time)
            print(' Warning:' + print_prefix + ' does not contain any of the selected fields.', file=sys.stderr)
            time.sleep(sleep_time)
            print(print_prefix + ' -------------------------')
            continue
        probe_longitudes = probes_longitudes[in_csv_row]
        probe_latitudes = probes_latitudes[in_csv_row]
        probe_record_starts = probes_record_starts[in_csv_row]
        probe_record_ends = probes_record_ends[in_csv_row]
        probe_sampling_times = probes_sampling_times[in_csv_row]
        probe_depths = probes_depths[in_csv_row]
        probe_quality_controls = probes_quality_controls[in_csv_row]
        probe_notes = probes_notes[in_csv_row]
        out_file_name = 'insitu-data_' + platform_code
        out_standard_names = list()
        mean_longitudes = list()
        mean_latitudes = list()
        start_dates = list()
        end_dates = list()
        sampling_times = list()
        valid_depth_levels = list()
        quality_controls = list()
        notes_array = list()
        if routine_qc_iterations >= 0:
            rejection_standard_names = list()
            out_stat = dict()
        for variable_standard_name in intersection_standard_names:
            print(print_prefix + ' analyzing variable ' + variable_standard_name + '...')
            variable_index = probe_standard_names.index(variable_standard_name)
            print(print_prefix + ' -------------------------')
            print(print_prefix + ' processing field standard name: ' + variable_standard_name)
            probe_longitude = probe_longitudes[variable_index]
            print(print_prefix + ' mean sensor latitude : ' + probe_longitude + ' degrees east')
            probe_latitude = probe_latitudes[variable_index]
            print(print_prefix + ' mean sensor latitude : ' + probe_latitude + ' degrees west')
            probe_record_start = probe_record_starts[variable_index]
            out_of_area = False
            if (not west_boundary < np.float32(probe_longitude) < east_boundary) or \
                    (not south_boundary < np.float32(probe_latitude) < north_boundary):
                out_of_area = True
            if ((biscay_gulf_min_lon < np.float32(probe_longitude) < biscay_gulf_max_lon) and
                    (biscay_gulf_min_lat < np.float32(probe_latitude) < biscay_gulf_max_lat)) and biscay_gulf_masking:
                out_of_area = True
            if ((marmara_sea_min_lon < np.float32(probe_longitude) < marmara_sea_max_lon) and
                    (marmara_sea_min_lat < np.float32(probe_latitude) < marmara_sea_max_lat)) and marmara_sea_masking:
                out_of_area = True
            if ((black_sea_min_lon < np.float32(probe_longitude) < black_sea_max_lon) and
                    (black_sea_min_lat < np.float32(probe_latitude) < black_sea_max_lat)) and black_sea_masking:
                out_of_area = True
            if ((azov_sea_min_lon < np.float32(probe_longitude) < azov_sea_max_lon) and
                    (azov_sea_min_lat < np.float32(probe_latitude) < azov_sea_max_lat)) and azov_sea_masking:
                out_of_area = True
            if out_of_area:
                time.sleep(sleep_time)
                print(' Warning:' + print_prefix + ' in situ device location is outside the selected area.',
                      file=sys.stderr)
                time.sleep(sleep_time)
                print(print_prefix + ' -------------------------')
                continue
            print(print_prefix + ' start recording time: ' + probe_record_start)
            probe_start_date = time.strptime(probe_record_start, '%Y-%m-%d %H:%M:%S')
            probe_record_end = probe_record_ends[variable_index]
            print(print_prefix + ' end recording time: ' + probe_record_end)
            probe_end_date = time.strptime(probe_record_end, '%Y-%m-%d %H:%M:%S')
            if first_date_str is not None:
                if first_date > probe_end_date:
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix + ' last probe record before selected first date.',
                          file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    continue
            if last_date_str is not None:
                if probe_start_date > last_date:
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix + ' first probe record after selected last date.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    continue
            probe_sampling_time = probe_sampling_times[variable_index]
            print(print_prefix + ' most representative sampling time: ' + probe_sampling_time + ' HH:MM:SS')
            try:
                field_quality_controls_str = probe_quality_controls[variable_index]
            except IndexError:
                field_quality_controls_str = probe_quality_controls[0]
            try:
                field_notes_str = probe_notes[variable_index]
            except IndexError:
                field_notes_str = probe_notes[0]
            in_field_dir = in_dir + '/' + variable_standard_name + '/'
            out_field_file_name = out_file_name + '_' + variable_standard_name
            file_list = [in_field_dir + '/' + file for file in os.listdir(in_field_dir)
                         if file.startswith(out_field_file_name)]
            if not file_list:
                time.sleep(sleep_time)
                print(' Warning:' + print_prefix + ' pre processed file for variable ' + variable_standard_name +
                      ' not found in input directory. Skipping...', file=sys.stderr)
                time.sleep(sleep_time)
                print(print_prefix + ' -------------------------')
                continue
            in_file = file_list[0]
            probe_depth = ' '.join(probe_depths[variable_index])
            if probe_depth == 'floating':
                is_floating_probe = True
            else:
                is_floating_probe = False
                [depth_information_array, out_depth_levels] = depth_calc.depth_calc(in_file, variable_standard_name,
                                                                                    verbose=False)
                if len(out_depth_levels) == 0:
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix + ' no valid depth levels for variable ' + variable_standard_name +
                          '. Skipping...', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    continue
                probe_depth = ' '.join(list(map(str, out_depth_levels)))
                print(print_prefix + ' Output depths for this variable: ' + probe_depth + ' meters.')
            in_data = netCDF4.Dataset(in_file, mode='r')
            depth_dimension_length = len(in_data.dimensions['depth'])
            print(print_prefix + ' input file depth levels: ' + str(depth_dimension_length))
            in_record_dimension = in_data.dimensions['time'].size
            in_data.close()
            print(print_prefix + ' record coordinate size: ' + str(in_record_dimension) + ' records.')
            work_field_dir = work_dir + '/' + variable_standard_name + '/'
            if not os.path.exists(work_field_dir):
                print(print_prefix + ' Creating work ' + variable_standard_name + ' data folder.')
                os.makedirs(work_field_dir)

            if not is_floating_probe:
                depth_aggregated_file = work_field_dir + out_field_file_name + '_depth-aggregated.nc'
                depth_aggregator.depth_aggregator(in_file, probe_depth, depth_aggregated_file,
                                                  first_date_str, last_date_str, verbose=verbose)
                if not os.path.isfile(depth_aggregated_file):
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix + ' depth aggregated file not produced.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    continue
                depth_aggregated_data = netCDF4.Dataset(depth_aggregated_file, mode='r+')
                out_depth = depth_aggregated_data.variables['depth']
                out_depth_cell_methods = ''
                if update_mode:
                    out_depth_cell_methods = 'depth: rescaled on predefined depth levels'
                elif ('rescaled depth' not in field_notes_str) and \
                        ('rounded depth levels' not in field_notes_str):
                    out_depth_cell_methods = 'time: mean and rearrange'
                if ('rescaled depth' in field_notes_str) or ('rounded depth levels' in field_notes_str):
                    if out_depth_cell_methods == '':
                        out_depth_cell_methods = 'depth: rescaled'
                    else:
                        out_depth_cell_methods += ' depth: rescaled'
                if out_depth_cell_methods != '':
                    out_depth.cell_methods = out_depth_cell_methods
                try:
                    out_history = depth_aggregated_data.history
                except AttributeError:
                    out_history = ''

                if ('rescaled depth' in field_notes_str) or \
                        ('rounded depth levels' in field_notes_str):
                    out_history = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) \
                                  + ' : Computed depth dimension average\n' + out_history

                depth_aggregated_data.history = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + \
                    ' : Created depth time constant dimension variable\n' + out_history
                if update_mode:
                    depth_aggregated_data.history = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + \
                        ' : Computed depth dimension average on predefined depth levels\n' + out_history
                depth_aggregated_data.close()
            else:
                shutil.copy2(in_file, depth_aggregated_file)

            print(print_prefix + ' checking time step.')
            time_step_check = time_check.time_check(depth_aggregated_file, verbose=False)
            if time_step_check == 0:
                print(print_prefix +
                      ' Time monotonically increases without duplicates'
                      ' between one time step and another.')
            elif time_step_check == 1:
                time.sleep(sleep_time)
                print(' Warning:' + print_prefix +
                      ' duplicated time records for field ' + variable_standard_name + '.', file=sys.stderr)
                time.sleep(sleep_time)
            elif time_step_check == 2:
                time.sleep(sleep_time)
                print(' Warning:' + print_prefix +
                      ' wrong positioning records for field ' + variable_standard_name + '.', file=sys.stderr)
                time.sleep(sleep_time)
            elif time_step_check == 3:
                time.sleep(sleep_time)
                print(' Warning:' + print_prefix +
                      ' duplicated entries and wrong positioning records for field '
                      + variable_standard_name + '.', file=sys.stderr)
                time.sleep(sleep_time)
            no_duplicates_file = work_field_dir + out_field_file_name + '_no-duplicates.nc'
            if (time_step_check == 1) or (time_step_check == 3):
                print(print_prefix + ' removing time step duplicates.')
                duplicated_records_remover.duplicated_records_remover(depth_aggregated_file, no_duplicates_file,
                                                                      verbose=verbose)
            else:
                shutil.copy2(depth_aggregated_file, no_duplicates_file)
            if not os.path.isfile(no_duplicates_file):
                time.sleep(sleep_time)
                print(' Warning:' + print_prefix + ' no duplicates file not produced.', file=sys.stderr)
                time.sleep(sleep_time)
                print(print_prefix + ' -------------------------')
                continue
            in_situ_raw_file = work_field_dir + out_field_file_name + '_raw.nc'
            if (time_step_check == 2) or (time_step_check == 3):
                print(print_prefix + ' fixing time step monotonicity.')
                records_monotonicity_fixer.records_monotonicity_fixer(no_duplicates_file, in_situ_raw_file,
                                                                      verbose=verbose)
            else:
                shutil.copy2(no_duplicates_file, in_situ_raw_file)
            if not os.path.isfile(in_situ_raw_file):
                time.sleep(sleep_time)
                print(' Warning:' + print_prefix + ' in situ raw file not produced.', file=sys.stderr)
                time.sleep(sleep_time)
                print(print_prefix + ' -------------------------')
                continue

            if routine_qc_iterations >= 0:
                quality_checked_file = work_field_dir + out_field_file_name + '_quality-checked.nc'
                if update_mode:
                    if not os.path.exists(climatology_dir):
                        time.sleep(sleep_time)
                        print(' Error. platform climatology directory not found.', file=sys.stderr)
                        time.sleep(sleep_time)
                        print(' -------------------------')
                        return
                else:
                    climatology_dir = out_dir + '/climatology/'
                    if not os.path.exists(climatology_dir):
                        print(' Creating platform climatology directory.')
                        print(' -------------------------')
                        os.makedirs(climatology_dir)
                climatology_field_dir = climatology_dir + '/' + variable_standard_name + '/'
                if not os.path.exists(climatology_field_dir) and not update_mode:
                    print(print_prefix + ' Creating climatology ' + variable_standard_name + ' data folder.')
                    os.makedirs(climatology_field_dir)
                climatology_file = climatology_field_dir + '/' + out_field_file_name + '_climatology.nc'
                if update_mode and not os.path.isfile(climatology_file):
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix + ' Climatology file not present in climatology directory.',
                          file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    continue
                post_processed_file = work_field_dir + out_field_file_name + '_post-processed.nc'
                print(print_prefix + ' Producing post processed field file for analysis...')
                out_stat_variable = \
                    time_series_post_processing.time_series_post_processing(in_situ_raw_file,
                                                                            variable_standard_name,
                                                                            update_mode,
                                                                            routine_qc_iterations,
                                                                            climatology_file,
                                                                            post_processed_file,
                                                                            verbose=verbose)
                if not os.path.isfile(post_processed_file) or not out_stat_variable:
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix + ' post processed file not produced.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    continue
                quality_check_applier.quality_check_applier(post_processed_file, variable_standard_name, "1",
                                                            quality_checked_file, routine_qc_iterations,
                                                            verbose=verbose)
                if not os.path.isfile(quality_checked_file):
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix + ' quality checked file not produced for field '
                          + variable_standard_name + '.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    continue
                rejection_standard_names.append(variable_standard_name)
                if not out_stat:
                    out_stat = out_stat_variable
                else:
                    for stat_column in out_stat_variable.keys():
                        out_stat[stat_column] += ';' + out_stat_variable[stat_column]

                quality_checked_data = netCDF4.Dataset(quality_checked_file, mode='r')
                quality_checked_time_data = quality_checked_data.variables['time'][...]
                quality_checked_data.close()
                start_date_seconds = np.min(quality_checked_time_data)
                probe_record_start = str(time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(start_date_seconds)))
                end_date_seconds = np.max(quality_checked_time_data)
                probe_record_end = str(time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(end_date_seconds)))
                probe_sampling_time_seconds = time_calc.time_calc(quality_checked_file, verbose=False)
                probe_sampling_time_days = probe_sampling_time_seconds // 86400
                probe_sampling_time_modulus = probe_sampling_time_seconds - probe_sampling_time_days * 86400
                probe_sampling_time_hours = time.strftime('%H:%M:%S', time.gmtime(probe_sampling_time_modulus))
                if probe_sampling_time_days < 9:
                    probe_sampling_time = '00' + str(probe_sampling_time_days) + ' ' + probe_sampling_time_hours
                elif probe_sampling_time_days < 99:
                    probe_sampling_time = '0' + str(probe_sampling_time_days) + ' ' + probe_sampling_time_hours
                elif probe_sampling_time_days < 999:
                    probe_sampling_time = str(probe_sampling_time_days) + ' ' + probe_sampling_time_hours

            out_standard_names.append(variable_standard_name)
            mean_longitudes.append(probe_longitude)
            mean_latitudes.append(probe_latitude)
            start_dates.append(probe_record_start)
            end_dates.append(probe_record_end)
            sampling_times.append(probe_sampling_time)
            valid_depth_levels.append(probe_depth)
            quality_controls.append(field_quality_controls_str)
            notes_array.append(field_notes_str)

        if len(out_standard_names) > 0:
            if probe_type not in out_devices_data[:, 1]:
                device_id += 1
                out_devices_line = np.array([[device_id, probe_type]], dtype=object)
                out_devices_data = np.append(out_devices_data, out_devices_line, axis=0)
                print(print_prefix + ' Writing output devices CSV file...')
                np.savetxt(out_devices_file, out_devices_data, fmt='"%s"', delimiter=',', comments='')
            out_device_ids = out_devices_data[:, 0]
            out_device_types = out_devices_data[:, 1]
            out_device_index = np.where(out_device_types == probe_type)[0][0]
            out_device_id = out_device_ids[out_device_index]

            if probe_organization not in out_organizations_data[:, 1]:
                organization_id += 1
                out_organizations_line = \
                    np.array([[organization_id, probe_organization, organization_country, organization_link]],
                             dtype=object)
                out_organizations_data = np.append(out_organizations_data, out_organizations_line, axis=0)
                print(print_prefix + ' Writing output organizations CSV file...')
                np.savetxt(out_organizations_file, out_organizations_data,
                           fmt='"%s"', delimiter=',', comments='')
            out_organization_ids = out_organizations_data[:, 0]
            out_organization_names = out_organizations_data[:, 1]
            out_organization_index = np.where(out_organization_names == probe_organization)[0][0]
            out_organization_id = out_organization_ids[out_organization_index]

            probe_id += 1
            variable_ids = []
            for variable_standard_name in out_standard_names:
                variable_index = in_fields_standard_name_list.index(variable_standard_name)
                variable_ids.append(str(in_variable_ids[variable_index]))
                out_variable_ids = out_variables_data[:, 0]
                if in_variable_ids[variable_index] not in out_variable_ids:
                    standard_name_index = np.where(variable_standard_names == variable_standard_name)[0][0]

                    variable_long_name = variable_standard_names[standard_name_index]
                    variable_unit = variable_units[standard_name_index]
                    out_variables_line = np.array(
                        [[in_variable_ids[variable_index], variable_standard_name,
                          variable_long_name, variable_unit]], dtype=object)
                    out_variables_data = np.append(out_variables_data, out_variables_line, axis=0)
                    print(print_prefix + ' Writing output variables CSV file...')
                    np.savetxt(out_variables_file, out_variables_data, fmt='"%s"', delimiter=',',
                               comments='')

            out_probes_line = \
                np.array([[probe_id, platform_code, probe_name, probe_wmo, out_device_id, out_organization_id,
                           ';'.join(variable_ids),
                           ';'.join(mean_longitudes), ';'.join(mean_latitudes),
                           ';'.join(start_dates), ';'.join(end_dates),
                           ';'.join(sampling_times), ';'.join(valid_depth_levels),
                           ';'.join(quality_controls), ';'.join(notes_array), organization_link]], dtype=object)
            # out_probes_line = \
            #     np.array([[probe_id, platform_code, out_platform_name, wmo, platform_type, organization_name,
            #                ';'.join(out_standard_names),
            #                ';'.join(mean_longitudes), ';'.join(mean_latitudes),
            #                ';'.join(start_dates), ';'.join(end_dates),
            #                ';'.join(sampling_times), ';'.join(valid_depth_levels),
            #                ';'.join(quality_controls), ';'.join(notes_array), organization_link]], dtype=object)

            out_probes_data = np.append(out_probes_data, out_probes_line, axis=0)
            print(print_prefix + ' Writing output probes CSV file...')
            np.savetxt(out_probes_file, out_probes_data, fmt='"%s"', delimiter=',', comments='')

            if routine_qc_iterations >= 0:
                out_rejection_line = \
                    np.array([[platform_code, ';'.join(rejection_standard_names), out_stat['data_total'],
                               out_stat['filled_data']]], dtype=object)
                if global_range_check_enabled:
                    try:
                        out_rejection_line = np.append(out_rejection_line,
                                                       np.array([[out_stat['range_check_rejection']]]), axis=1)
                    except KeyError:
                        out_stat_length = len(out_stat['data_total'].split(';'))
                        out_stat_line = ';'.join(map(str, np.zeros(out_stat_length, dtype=int).tolist()))
                        out_rejection_line = np.append(out_rejection_line, np.array([[out_stat_line]]), axis=1)
                if spike_test_enabled:
                    out_rejection_line = np.append(out_rejection_line,
                                                   np.array([[out_stat['spike_test_rejection']]]), axis=1)
                if stuck_value_test_enabled:
                    out_rejection_line = np.append(out_rejection_line,
                                                   np.array([[out_stat['stuck_value_rejection']]]), axis=1)
                if routine_qc_iterations >= 1:
                    for iteration in range(1, routine_qc_iterations + 1):
                        out_rejection_line = np.append(out_rejection_line,
                                                       np.array([[out_stat['statistic_rejection_' + str(iteration)]]]),
                                                       axis=1)

                out_rejection_data = np.append(out_rejection_data, out_rejection_line, axis=0)
                print(print_prefix + ' Writing output rejection CSV file...')
                np.savetxt(out_rejection_file, out_rejection_data, fmt='"%s"', delimiter=',', comments='')

        for variable_standard_name in out_standard_names:
            work_field_dir = work_dir + '/' + variable_standard_name + '/'
            out_field_file_name = out_file_name + '_' + variable_standard_name
            in_situ_raw_file = work_field_dir + out_field_file_name + '_raw.nc'
            if not os.path.isfile(in_situ_raw_file):
                in_situ_raw_file = work_field_dir + out_field_file_name + '_derived.nc'
                if not os.path.isfile(in_situ_raw_file):
                    continue
            out_raw_dir = out_dir + '/raw/'
            if not os.path.exists(out_raw_dir):
                print(' Creating raw data folder.')
                os.makedirs(out_raw_dir)
            out_raw_field_dir = out_raw_dir + variable_standard_name + '/'
            if not os.path.exists(out_raw_field_dir):
                print(' Creating ' + variable_standard_name + ' raw data folder.')
                os.makedirs(out_raw_field_dir)
            out_raw_file = out_raw_field_dir + out_field_file_name + '_raw.nc'
            print(print_prefix + ' copying raw file to output directory.')
            shutil.copy2(in_situ_raw_file, out_raw_file)
            if routine_qc_iterations >= 0:
                quality_checked_file = work_field_dir + out_field_file_name + '_quality-checked.nc'
                if not os.path.isfile(quality_checked_file):
                    quality_checked_file = work_field_dir + out_field_file_name + '_derived.nc'
                    if not os.path.isfile(quality_checked_file):
                        continue
                out_quality_checked_dir = out_dir + '/quality-checked/'
                if not os.path.exists(out_quality_checked_dir):
                    print(' Creating quality checked data folder.')
                    os.makedirs(out_quality_checked_dir)
                out_quality_checked_field_dir = out_quality_checked_dir + variable_standard_name + '/'
                if not os.path.exists(out_quality_checked_field_dir):
                    print(' Creating ' + variable_standard_name + ' quality checked data folder.')
                    os.makedirs(out_quality_checked_field_dir)
                out_quality_checked_file = out_quality_checked_field_dir + out_field_file_name + '_quality-checked.nc'
                print(print_prefix + ' copying file to output directory.')
                shutil.copy2(quality_checked_file, out_quality_checked_file)
                to_average_file = quality_checked_file
            else:
                to_average_file = in_situ_raw_file
            to_average_sampling_time_seconds = time_calc.time_calc(to_average_file, verbose=False)
            under_one_hour = True
            under_one_day = True
            under_one_month = True
            under_one_year = True
            if to_average_sampling_time_seconds > (1 + time_step_tolerance / 100) * 3600:
                under_one_hour = False
            if to_average_sampling_time_seconds > (1 + time_step_tolerance / 100) * 3600 * 24:
                under_one_day = False
            if to_average_sampling_time_seconds > (1 + time_step_tolerance / 100) * 3600 * 24 * 31:
                under_one_month = False
            if to_average_sampling_time_seconds > (1 + time_step_tolerance / 100) * 3600 * 24 * 365:
                under_one_year = False
            for record_type in record_folders:
                if (record_type == 'hm') and not under_one_hour:
                    continue
                if ((record_type == 'dm') or (record_type == 'dm_shift')) and not under_one_day:
                    continue
                if (record_type == 'mm') and not under_one_month:
                    continue
                if (record_type == 'ym') and not under_one_year:
                    continue
                out_record_dir = out_dir + '/' + record_type + '/'
                if not os.path.exists(out_record_dir):
                    print(' Creating ' + record_type + ' data folder.')
                    os.makedirs(out_record_dir)
                out_field_dir = out_record_dir + variable_standard_name + '/'
                if not os.path.exists(out_field_dir):
                    print(' Creating ' + variable_standard_name + ' ' + record_type + ' data folder.')
                    os.makedirs(out_field_dir)
                averaged_file = work_field_dir + out_field_file_name + '_' + record_type + '.nc'
                out_averaged_file = out_field_dir + out_field_file_name + '_' + record_type + '.nc'
                if record_type == 'hm':
                    out_record_time_str = '01:00:00'
                elif (record_type == 'dm') or (record_type == 'dm_shift'):
                    out_record_time_str = '24:00:00'
                elif record_type == 'mm':
                    out_record_time_str = '01'
                elif record_type == 'ym':
                    out_record_time_str = '12'
                if record_type == 'dm_shift':
                    half_time_step_shift = True
                else:
                    half_time_step_shift = False
                print(print_prefix + ' computing weighted ' + record_type + ' time average.')
                time_averager.time_averager(to_average_file, out_record_time_str,
                                            averaged_file, variable_standard_name,
                                            half_time_step_shift=half_time_step_shift, verbose=verbose)
                if os.path.isfile(averaged_file):
                    print(print_prefix + ' copying file to output directory.')
                    shutil.copy2(averaged_file, out_averaged_file)
                else:
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix + ' ' + record_type + ' mean files not produced for field '
                          + variable_standard_name + '.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    continue

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
        in_csv_dir = sys.argv[1]
        in_dir = sys.argv[2]
        in_fields_standard_name_str = sys.argv[3]
        work_dir = sys.argv[4]
        out_dir = sys.argv[5]
        routine_qc_iterations = int(sys.argv[6])
    except (IndexError, ValueError):
        in_csv_dir = None
        in_dir = None
        in_fields_standard_name_str = None
        work_dir = None
        out_dir = None
        routine_qc_iterations = None

    try:
        climatology_dir = sys.argv[7]
    except (IndexError, ValueError):
        climatology_dir = None

    try:
        first_date_str = sys.argv[8]
    except (IndexError, ValueError):
        first_date_str = None

    try:
        last_date_str = sys.argv[9]
    except (IndexError, ValueError):
        last_date_str = None

    try:
        region_boundaries_str = sys.argv[10]
    except (IndexError, ValueError):
        region_boundaries_str = None

    try:
        med_sea_masking = string_to_bool(sys.argv[11])
    except (IndexError, ValueError):
        med_sea_masking = False

    try:
        in_instrument_types_str = sys.argv[12]
    except (IndexError, ValueError):
        in_instrument_types_str = None

    try:
        verbose = string_to_bool(sys.argv[13])
    except (IndexError, ValueError):
        verbose = True

    obs_postpro(in_csv_dir, in_dir, in_fields_standard_name_str, work_dir, out_dir,
                routine_qc_iterations, climatology_dir, first_date_str, last_date_str,
                region_boundaries_str, med_sea_masking, in_instrument_types_str, verbose)
