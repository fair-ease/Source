# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
import numpy as np
import time
import calendar

# Global variables
sleep_time = 0.1  # seconds


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Load input arguments
try:
    base_csv_dir = sys.argv[1]
    update_csv_dir = sys.argv[2]
    merged_csv_dir = sys.argv[3]
except (IndexError, ValueError):
    base_csv_dir = None
    update_csv_dir = None
    merged_csv_dir = None

try:
    base_is_dominant = string_to_bool(sys.argv[4])
except (IndexError, ValueError):
    base_is_dominant = True

try:
    verbose = string_to_bool(sys.argv[5])
except (IndexError, ValueError):
    verbose = True


# Functional version
def metadata_merger(base_csv_dir=None, update_csv_dir=None, merged_csv_dir=None, base_is_dominant=True, verbose=True):
    """
    Script to merge two SOURCE CSV database directories, with option of first one dominant.

    Input arguments:

        1) Base in situ information CSV directory: Directory with (almost) the sequent files):
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

        2) Update in situ information CSV directory: Directory with (almost) the sequent files):
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

        3) Dominant merge method (OPTIONAL);

        4) verbosity switch (OPTIONAL).

    Output:

        1) Merged in situ information CSV directory.

    Written Dec 10, 2018 by Paolo Oliveri
    """
    # if __name__ == '__main__':
    #     return
    start_run_time = calendar.timegm(time.gmtime())
    print('-------------------------' + ' ' + __file__ + ' -------------------------')
    print(' Local time is ' + time.strftime("%a %b %d %H:%M:%S %Z %Y", time.gmtime()))
    print(' -------------------------')
    print(' Merger for SOURCE CSV databases.')
    print(' -------------------------')
    if base_csv_dir is None or update_csv_dir is None or merged_csv_dir is None:
        time.sleep(sleep_time)
        print(' ERROR: 3 of 5 maximum arguments (2 optionals) not provided.', file=sys.stderr)
        print(' 1) Base in situ information CSV directory;', file=sys.stderr)
        print(' 2) Update in situ information CSV directory;', file=sys.stderr)
        print(' 3) Merged in situ information CSV directory;', file=sys.stderr)
        print(' 4) (optional) base directory dominant in merge process (True or False) (default: True).',
              file=sys.stderr)
        print(' 5) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return

    print(' Base in situ information CSV directory = ' + base_csv_dir)
    print(' Update in situ information CSV directory = ' + update_csv_dir)
    print(' Merged in situ information CSV directory = ' + merged_csv_dir)
    print(' base file dominant in merge process switch = ' + str(base_is_dominant))
    print(' verbosity switch = ' + str(verbose))
    print(' -------------------------')
    print(' Starting process...')
    print(' -------------------------')

    print(' Loading base devices information CSV file...')
    base_devices_file = base_csv_dir + '/devices.csv'
    try:
        base_devices_data = open(base_devices_file, 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty base devices CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    base_devices_data =\
        pd.read_csv(base_devices_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if base_devices_data.ndim == 1:
        base_devices_data = base_devices_data[np.newaxis, :]

    base_devices_ids = base_devices_data[:, 0]
    base_devices_types = base_devices_data[:, 1]

    print(' Loading base organizations information CSV file...')
    base_organizations_file = base_csv_dir + '/organizations.csv'
    try:
        base_organizations_data = open(base_organizations_file, 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty base organizations CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    base_organizations_data =\
        pd.read_csv(base_organizations_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if base_organizations_data.ndim == 1:
        base_organizations_data = base_organizations_data[np.newaxis, :]

    base_organizations_ids = base_organizations_data[:, 0]
    base_organizations_names = base_organizations_data[:, 1]
    base_organizations_countries = base_organizations_data[:, 2]
    base_organizations_links = base_organizations_data[:, 3]

    print(' Loading base variables information CSV file...')
    base_variables_file = base_csv_dir + '/variables.csv'
    try:
        base_variables_data = open(base_variables_file, 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty base variables CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    base_variables_data =\
        pd.read_csv(base_variables_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if base_variables_data.ndim == 1:
        base_variables_data = base_variables_data[np.newaxis, :]

    base_variables_ids = base_variables_data[:, 0]
    base_variables_standard_names = base_variables_data[:, 1]
    base_variables_long_names = base_variables_data[:, 2]
    base_variables_units = base_variables_data[:, 3]

    print(' Loading base probes information CSV file...')
    base_probes_file = base_csv_dir + '/probes.csv'
    try:
        base_probes_data = open(base_probes_file, 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty base probes CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    base_probes_data =\
        pd.read_csv(base_probes_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if base_probes_data.ndim == 1:
        base_probes_data = base_probes_data[np.newaxis, :]

    base_probes_ids = base_probes_data[:, 0]
    base_probes_platform_codes = base_probes_data[:, 1]
    base_probes_names = base_probes_data[:, 2]
    base_probes_wmo = base_probes_data[:, 3]
    base_probes_device_type_ids = base_probes_data[:, 4]
    base_probes_types = np.copy(base_probes_device_type_ids)
    for index in range(len(base_devices_types)):
        base_probes_types = np.where(base_probes_types == base_devices_ids[index],
                                     base_devices_types[index], base_probes_types)
    base_probes_organization_ids = base_probes_data[:, 5]
    base_probes_organizations = np.copy(base_probes_organization_ids)
    for index in range(len(base_organizations_names)):
        base_probes_organizations =\
            np.where(base_probes_organizations == base_organizations_ids[index],
                     base_organizations_names[index], base_probes_organizations)
    base_probes_variables_ids = [standard_names.split(';') for standard_names in base_probes_data[:, 6]]
    base_probes_standard_names = base_probes_variables_ids
    for index in range(len(base_probes_variables_ids)):
        for index_id in range(len(base_variables_ids)):
            try:
                probe_variable_index = base_probes_standard_names[index].index(base_variables_ids[index_id])
                base_probes_standard_names[index][probe_variable_index] = base_variables_standard_names[index_id]
            except ValueError:
                continue
    base_probes_longitudes = [longitudes.split(';') for longitudes in base_probes_data[:, 7]]
    base_probes_latitudes = [latitudes.split(';') for latitudes in base_probes_data[:, 8]]
    base_probes_record_starts = [record_starts.split(';') for record_starts in base_probes_data[:, 9]]
    base_probes_record_ends = [record_ends.split(';') for record_ends in base_probes_data[:, 10]]
    base_probes_sampling_times = [sampling_times.split(';') for sampling_times in base_probes_data[:, 11]]
    base_probes_depths = [[depths.split(' ') for depths in split_depths.split(';')]
                          for split_depths in base_probes_data[:, 12]]
    base_probes_quality_controls = [quality_controls.split(';') for quality_controls in base_probes_data[:, 13]]
    base_probes_notes = [notes.split(';') for notes in base_probes_data[:, 14]]
    base_probes_links = base_probes_data[:, 15]

    print(' Loading update devices information CSV file...')
    update_devices_file = update_csv_dir + '/devices.csv'
    try:
        update_devices_data = open(update_devices_file, 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty update devices CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    update_devices_data =\
        pd.read_csv(update_devices_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if update_devices_data.ndim == 1:
        update_devices_data = update_devices_data[np.newaxis, :]

    update_devices_ids = update_devices_data[:, 0]
    update_devices_types = update_devices_data[:, 1]

    print(' Loading update organizations information CSV file...')
    update_organizations_file = update_csv_dir + '/organizations.csv'
    try:
        update_organizations_data = open(update_organizations_file, 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty update organizations CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    update_organizations_data =\
        pd.read_csv(update_organizations_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if update_organizations_data.ndim == 1:
        update_organizations_data = update_organizations_data[np.newaxis, :]

    update_organizations_ids = update_organizations_data[:, 0]
    update_organizations_names = update_organizations_data[:, 1]
    update_organizations_countries = update_organizations_data[:, 2]
    update_organizations_links = update_organizations_data[:, 3]

    print(' Loading update variables information CSV file...')
    update_variables_file = update_csv_dir + '/variables.csv'
    try:
        update_variables_data = open(update_variables_file, 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty update variables CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    update_variables_data =\
        pd.read_csv(update_variables_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if update_variables_data.ndim == 1:
        update_variables_data = update_variables_data[np.newaxis, :]

    update_variables_ids = update_variables_data[:, 0]
    update_variables_standard_names = update_variables_data[:, 1]
    update_variables_long_names = update_variables_data[:, 2]
    update_variables_units = update_variables_data[:, 3]

    print(' Loading update probes information CSV file...')
    update_probes_file = update_csv_dir + '/probes.csv'
    try:
        update_probes_data = open(update_probes_file, 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty update probes CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    update_probes_data =\
        pd.read_csv(update_probes_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if update_probes_data.ndim == 1:
        update_probes_data = update_probes_data[np.newaxis, :]

    update_probes_platform_codes = update_probes_data[:, 1]
    update_probes_names = update_probes_data[:, 2]
    update_probes_wmo = update_probes_data[:, 3]
    update_probes_device_type_ids = update_probes_data[:, 4]
    update_probes_types = np.copy(update_probes_device_type_ids)
    for index in range(len(update_devices_types)):
        update_probes_types = np.where(update_probes_types == update_devices_ids[index],
                                       update_devices_types[index], update_probes_types)
    update_probes_organization_ids = update_probes_data[:, 5]
    update_probes_organizations = np.copy(update_probes_organization_ids)
    for index in range(len(update_organizations_names)):
        update_probes_organizations =\
            np.where(update_probes_organizations == update_organizations_ids[index],
                     update_organizations_names[index], update_probes_organizations)
    update_probes_variables_ids = [standard_names.split(';') for standard_names in update_probes_data[:, 6]]
    update_probes_standard_names = update_probes_variables_ids
    for index in range(len(update_probes_variables_ids)):
        for index_id in range(len(update_variables_ids)):
            try:
                probe_variable_index = update_probes_standard_names[index].index(update_variables_ids[index_id])
                update_probes_standard_names[index][probe_variable_index] = update_variables_standard_names[index_id]
            except ValueError:
                continue
    update_probes_longitudes = [longitudes.split(';') for longitudes in update_probes_data[:, 7]]
    update_probes_latitudes = [latitudes.split(';') for latitudes in update_probes_data[:, 8]]
    update_probes_record_starts = [record_starts.split(';') for record_starts in update_probes_data[:, 9]]
    update_probes_record_ends = [record_ends.split(';') for record_ends in update_probes_data[:, 10]]
    update_probes_sampling_times = [sampling_times.split(';') for sampling_times in update_probes_data[:, 11]]
    update_probes_depths = [[depths.split(' ') for depths in split_depths.split(';')]
                            for split_depths in update_probes_data[:, 12]]
    update_probes_quality_controls = [quality_controls.split(';') for quality_controls in update_probes_data[:, 13]]
    update_probes_notes = [notes.split(';') for notes in update_probes_data[:, 14]]
    update_probes_links = update_probes_data[:, 15]

    update_devices_data_difference =\
        [device_type for device_type in update_devices_types if device_type not in base_devices_types]

    update_organization_names_difference =\
        [organization_name for organization_name in update_organizations_names
         if organization_name not in base_organizations_names]

    update_variable_standard_names_difference =\
        [standard_name for standard_name in update_variables_standard_names
         if standard_name not in base_variables_standard_names]

    update_probes_data_difference =\
        [platform_code for platform_code in update_probes_platform_codes
         if platform_code not in base_probes_platform_codes]

    if not os.path.exists(merged_csv_dir):
        print(' Creating output directory.')
        print(' -------------------------')
        os.makedirs(merged_csv_dir)
    if not os.listdir(merged_csv_dir):
        pass
    else:
        time.sleep(sleep_time)
        print(' Warning: existing files in output directory.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')

    merged_devices_file = merged_csv_dir + '/devices.csv'
    merged_devices_data = np.empty(shape=(1, 2), dtype=object)
    merged_devices_data[0, :] = ['id', 'name']
    print(' Writing merged devices CSV file header...')
    np.savetxt(merged_devices_file, merged_devices_data, fmt='"%s"', delimiter=',', comments='')

    merged_organizations_file = merged_csv_dir + '/organizations.csv'
    merged_organizations_data = np.empty(shape=(1, 4), dtype=object)
    merged_organizations_data[0, :] = ['id', 'name', 'country', 'link']
    print(' Writing merged organizations CSV file header...')
    np.savetxt(merged_organizations_file, merged_organizations_data, fmt='"%s"', delimiter=',', comments='')

    merged_variables_file = merged_csv_dir + '/variables.csv'
    merged_variables_data = np.empty(shape=(1, 4), dtype=object)
    merged_variables_data[0, :] = ['id', 'standard_name', 'long_name', 'units']
    print(' Writing merged variables CSV file header...')
    np.savetxt(merged_variables_file, merged_variables_data, fmt='"%s"', delimiter=',', comments='')

    # merged_probes_file = merged_csv_dir + '/probes.csv'
    merged_probes_data = np.empty(shape=(1, 16), dtype=object)
    merged_probes_data[0, :] = ['id', 'platform_code', 'name', 'wmo', 'device_id', 'organization_id', 'variable_ids',
                                'longitudes', 'latitudes', 'record_starts', 'record_ends',
                                'sampling_times', 'depths', 'quality_controls', 'notes', 'link']
    # print(' Writing merged probes CSV file header...')
    # np.savetxt(merged_probes_file, merged_probes_data, fmt='"%s"', delimiter=',', comments='')

    out_merged_probes_file = merged_csv_dir + '/probes.csv'
    out_merged_probes_data = np.empty(shape=(1, 16), dtype=object)
    out_merged_probes_data[0, :] = ['id', 'platform_code', 'name', 'wmo', 'device_id', 'organization_id',
                                    'variable_ids', 'longitudes', 'latitudes', 'record_starts', 'record_ends',
                                    'sampling_times', 'depths', 'quality_controls', 'notes', 'link']
    print(' Writing output merged probes CSV file header...')
    np.savetxt(out_merged_probes_file, out_merged_probes_data, fmt='"%s"', delimiter=',', comments='')

    if verbose:
        print(' -------------------------')
        print(' Local time is ' + time.strftime("%Y-%m-%d %H:%M:%S %Z", time.gmtime()))

    print(' -------------------------')
    print(' Analyzing base devices CSV file ' + base_devices_file)
    for base_devices_row in range(base_devices_data.shape[0]):
        print(' -------------------------')
        device_id = base_devices_ids[base_devices_row]
        device_type = base_devices_types[base_devices_row]
        print_prefix = ' (' + device_type + ')'
        merged_devices_line = np.array([[device_id, device_type]], dtype=object)
        merged_devices_data = np.append(merged_devices_data, merged_devices_line, axis=0)
        print(print_prefix + ' Writing merged devices CSV file...')
        np.savetxt(merged_devices_file, merged_devices_data, fmt='"%s"', delimiter=',', comments='')

    if not base_is_dominant:
        print(' -------------------------')
        print(' Analyzing also update devices CSV file ' + update_devices_file)
        print(' -------------------------')
        print(' Copying devices that are only present in this file.')
        merged_devices_ids_numeric = \
            np.array([int(device_id) for device_id in merged_devices_data[1:, 0]])
        update_device_id = np.max(merged_devices_ids_numeric)
        for device_type in update_devices_data_difference:
            update_device_id += 1
            print_prefix = ' (' + device_type + ')'
            print(print_prefix + ' Copying device type to merged devices CSV file.')
            merged_devices_line = np.array([[update_device_id, device_type]], dtype=object)
            merged_devices_data = np.append(merged_devices_data, merged_devices_line, axis=0)
            print(print_prefix + ' Writing merged organizations CSV file...')
            np.savetxt(merged_devices_file, merged_devices_data, fmt='"%s"', delimiter=',', comments='')

    print(' -------------------------')
    print(' Analyzing base organizations CSV file ' + base_organizations_file)
    for base_organizations_row in range(base_organizations_data.shape[0]):
        print(' -------------------------')
        organization_id = base_organizations_ids[base_organizations_row]
        organization_name = base_organizations_names[base_organizations_row]
        base_organization_country = base_organizations_countries[base_organizations_row]
        base_organization_link = base_organizations_links[base_organizations_row]
        print_prefix = ' (' + organization_name + ')'
        if organization_name in update_organizations_names:
            print(print_prefix + ' This data provider is present also in CSV file ' + update_organizations_file + '..')
            update_organizations_row = int(np.where(update_organizations_names == organization_name)[0][0])
            update_organization_country = update_organizations_countries[update_organizations_row]
            if update_organization_country == '':
                merged_organization_country = base_organization_country
            else:
                merged_organization_country = update_organization_country
            if update_organization_country != base_organization_country:
                time.sleep(sleep_time)
                print(print_prefix + ' Warning. The data provider countries in the two CSV files are different.',
                      file=sys.stderr)
                time.sleep(sleep_time)
            update_organization_link = update_organizations_links[update_organizations_row]
            if update_organization_link == '':
                merged_organization_link = base_organization_link
            else:
                merged_organization_link = update_organization_link
            if update_organization_link != base_organization_link:
                time.sleep(sleep_time)
                print(print_prefix + ' Warning. The data provider weblinks in the two CSV files are different.',
                      file=sys.stderr)
                time.sleep(sleep_time)
        else:
            merged_organization_country = base_organization_country
            merged_organization_link = base_organization_link
        merged_organizations_line = np.array([[organization_id, organization_name,
                                               merged_organization_country, merged_organization_link]], dtype=object)
        merged_organizations_data = np.append(merged_organizations_data, merged_organizations_line, axis=0)
        print(print_prefix + ' Writing merged organizations CSV file...')
        np.savetxt(merged_organizations_file, merged_organizations_data, fmt='"%s"', delimiter=',', comments='')

    if not base_is_dominant:
        print(' -------------------------')
        print(' Analyzing also update organizations CSV file ' + update_organizations_file)
        print(' -------------------------')
        print(' Copying organizations that are only present in this file.')
        merged_organizations_ids_numeric = \
            np.array([int(organization_id) for organization_id in merged_organizations_data[1:, 0]])
        update_organization_id = np.max(merged_organizations_ids_numeric)
        for organization_name in update_organization_names_difference:
            update_organization_id += 1
            print_prefix = ' (' + organization_name + ')'
            update_organizations_row = int(np.where(update_organizations_names == organization_name)[0][0])
            update_organizations_line = update_organizations_data[update_organizations_row, 1:]
            print(print_prefix + ' Copying organization name to merged organizations CSV file.')
            merged_organizations_line = np.array([[update_organization_id]], dtype=object)
            merged_organizations_line = np.append(merged_organizations_line, update_organizations_line[np.newaxis, :],
                                                  axis=1)
            merged_organizations_data = np.append(merged_organizations_data, merged_organizations_line, axis=0)
            print(print_prefix + ' Writing merged organizations CSV file...')
            np.savetxt(merged_organizations_file, merged_organizations_data, fmt='"%s"', delimiter=',', comments='')

    print(' -------------------------')
    print(' Analyzing base variables CSV file ' + base_variables_file)
    for base_variables_row in range(base_variables_data.shape[0]):
        print(' -------------------------')
        variable_id = base_variables_ids[base_variables_row]
        variable_standard_name = base_variables_standard_names[base_variables_row]
        base_variable_long_name = base_variables_long_names[base_variables_row]
        base_variable_units = base_variables_units[base_variables_row]
        print_prefix = ' (' + variable_standard_name + ')'
        if variable_standard_name in update_variables_standard_names:
            print(print_prefix + ' This variable is present also in CSV file ' + update_variables_file + '..')
            update_variables_row = int(np.where(update_variables_standard_names == variable_standard_name)[0][0])
            update_variable_long_name = update_variables_long_names[update_variables_row]
            if update_variable_long_name == '':
                merged_variable_long_name = base_variable_long_name
            else:
                merged_variable_long_name = update_variable_long_name
            if update_variable_long_name != base_variable_long_name:
                time.sleep(sleep_time)
                print(print_prefix + ' Warning. The variable long_names in the two CSV files are different.',
                      file=sys.stderr)
                time.sleep(sleep_time)
            update_variable_units = update_variables_units[update_variables_row]
            if update_variable_units == '':
                merged_variable_units = base_variable_units
            else:
                merged_variable_units = update_variable_units
            if update_variable_units != base_variable_units:
                time.sleep(sleep_time)
                print(print_prefix + ' Warning. The variables units in the two CSV files are different. Exiting...',
                      file=sys.stderr)
                time.sleep(sleep_time)
                return
        else:
            merged_variable_long_name = base_variable_long_name
            merged_variable_units = base_variable_units
        merged_variables_line = np.array([[variable_id, variable_standard_name,
                                           merged_variable_long_name, merged_variable_units]], dtype=object)
        merged_variables_data = np.append(merged_variables_data, merged_variables_line, axis=0)
        print(print_prefix + ' Writing merged variables CSV file...')
        np.savetxt(merged_variables_file, merged_variables_data, fmt='"%s"', delimiter=',', comments='')

    if not base_is_dominant:
        print(' -------------------------')
        print(' Analyzing also update variables CSV file ' + update_variables_file)
        print(' -------------------------')
        print(' Copying variables that are only present in this file.')
        merged_variables_ids_numeric = \
            np.array([int(variable_id) for variable_id in merged_variables_data[1:, 0]])
        update_variable_id = np.max(merged_variables_ids_numeric)
        for variable_standard_name in update_variable_standard_names_difference:
            update_variable_id += 1
            print_prefix = ' (' + variable_standard_name + ')'
            update_variables_row = int(np.where(update_variables_standard_names == variable_standard_name)[0][0])
            update_variables_line = update_variables_data[update_variables_row, 1:]
            print(print_prefix + ' Copying variable name to merged variables CSV file.')
            merged_variables_line = np.array([[str(update_variable_id)]], dtype=object)
            merged_variables_line = np.append(merged_variables_line, update_variables_line[np.newaxis, :], axis=1)
            merged_variables_data = np.append(merged_variables_data, merged_variables_line, axis=0)
            print(print_prefix + ' Writing merged variables CSV file...')
            np.savetxt(merged_variables_file, merged_variables_data, fmt='"%s"', delimiter=',', comments='')

    print(' -------------------------')
    print(' Analyzing base probes CSV file ' + base_probes_file)
    for base_probes_row in range(base_probes_data.shape[0]):
        print(' -------------------------')
        probe_id = base_probes_ids[base_probes_row]
        platform_code = base_probes_platform_codes[base_probes_row]
        print_prefix = ' (' + platform_code + ')'
        base_probe_name = base_probes_names[base_probes_row]
        base_probe_wmo = base_probes_wmo[base_probes_row]
        base_probe_type = base_probes_types[base_probes_row]
        base_probe_organization = base_probes_organizations[base_probes_row]
        base_probe_standard_names = base_probes_standard_names[base_probes_row]
        base_probe_longitudes = base_probes_longitudes[base_probes_row]
        base_probe_latitudes = base_probes_latitudes[base_probes_row]
        base_probe_record_starts = base_probes_record_starts[base_probes_row]
        base_probe_record_ends = base_probes_record_ends[base_probes_row]
        base_probe_sampling_times = base_probes_sampling_times[base_probes_row]
        base_probe_depths = base_probes_depths[base_probes_row]
        base_probe_quality_controls = base_probes_quality_controls[base_probes_row]
        base_probe_notes = base_probes_notes[base_probes_row]
        base_probe_link = base_probes_links[base_probes_row]
        print(print_prefix + ' probe name = \'' + base_probe_name + '\'')
        if platform_code in update_probes_platform_codes:
            print(print_prefix + ' This probe is present also in CSV file ' + update_probes_file +
                  '. Starting merge...')
            update_probes_row = int(np.where(update_probes_platform_codes == platform_code)[0][0])
            update_probe_name = update_probes_names[update_probes_row]
            if update_probe_name == '':
                merged_probe_name = base_probe_name
            else:
                merged_probe_name = update_probe_name
            if update_probe_name != base_probe_name:
                time.sleep(sleep_time)
                print(print_prefix + ' Warning. The platform names in the two CSV files are different.',
                      file=sys.stderr)
                time.sleep(sleep_time)
            update_probe_wmo = update_probes_wmo[update_probes_row]
            if update_probe_wmo == '':
                merged_probe_wmo = base_probe_wmo
            else:
                merged_probe_wmo = update_probe_wmo
            if update_probe_wmo != base_probe_wmo:
                time.sleep(sleep_time)
                print(print_prefix + ' Warning. The WMO platform codes in the two CSV files are different.',
                      file=sys.stderr)
                time.sleep(sleep_time)
            update_probe_type = update_probes_types[update_probes_row]
            if (update_probe_type == '') or base_is_dominant:
                merged_probe_type = base_probe_type
            else:
                merged_probe_type = update_probe_type
            if update_probe_type != base_probe_type:
                time.sleep(sleep_time)
                print(print_prefix + ' Warning. The probe types in the two CSV files are different.',
                      file=sys.stderr)
                time.sleep(sleep_time)
            update_probe_organization = update_probes_organizations[update_probes_row]
            if (update_probe_organization == '') or base_is_dominant:
                merged_probe_organization = base_probe_organization
            else:
                merged_probe_organization = update_probe_organization
            if update_probe_organization != base_probe_organization:
                time.sleep(sleep_time)
                print(print_prefix + ' Warning. The data providers in the two CSV files are different, skipping.',
                      file=sys.stderr)
                time.sleep(sleep_time)
            update_probe_standard_names = update_probes_standard_names[update_probes_row]
            update_probe_longitudes = update_probes_longitudes[update_probes_row]
            update_probe_latitudes = update_probes_latitudes[update_probes_row]
            update_probe_record_starts = update_probes_record_starts[update_probes_row]
            update_probe_record_ends = update_probes_record_ends[update_probes_row]
            update_probe_sampling_times = update_probes_sampling_times[update_probes_row]
            update_probe_depths = update_probes_depths[update_probes_row]
            update_probe_quality_controls = update_probes_quality_controls[update_probes_row]
            update_probe_notes = update_probes_notes[update_probes_row]
            update_probe_link = update_probes_links[update_probes_row]
            if update_probe_link == '':
                merged_probe_link = base_probe_link
            else:
                merged_probe_link = update_probe_link
            if update_probe_link != base_probe_link:
                time.sleep(sleep_time)
                print(print_prefix + ' Warning. The probe weblinks in the two CSV files are different.',
                      file=sys.stderr)
                time.sleep(sleep_time)
            update_standard_names_difference = \
                [standard_name for standard_name in update_probe_standard_names
                 if standard_name not in base_probe_standard_names]

            merged_probe_standard_names = list()
            merged_probe_longitudes = list()
            merged_probe_latitudes = list()
            merged_probe_record_starts = list()
            merged_probe_record_ends = list()
            merged_probe_sampling_times = list()
            merged_probe_depths = list()
            merged_probe_quality_controls = list()
            merged_probe_notes = list()

            for base_variable_number in range(len(base_probe_standard_names)):
                variable_standard_name = base_probe_standard_names[base_variable_number]
                print(print_prefix + ' Variable standard name ' + variable_standard_name + ' in ' + base_probes_file)
                base_longitude = float(base_probe_longitudes[base_variable_number])
                base_latitude = float(base_probe_latitudes[base_variable_number])
                base_record_start = pd.to_datetime(base_probe_record_starts[base_variable_number])
                base_record_end = pd.to_datetime(base_probe_record_ends[base_variable_number])
                base_sampling_time = base_probe_sampling_times[base_variable_number]
                try:
                    base_depths = np.array(base_probe_depths[base_variable_number], dtype=float)
                except ValueError:
                    base_depths = base_probe_depths[base_variable_number]
                base_quality_controls = base_probe_quality_controls[base_variable_number]
                base_notes = base_probe_notes[base_variable_number]
                if variable_standard_name in update_probe_standard_names:
                    variable_standard_name = base_probe_standard_names[base_variable_number]
                    print(print_prefix + ' The variable standard name ' + variable_standard_name
                          + ' is present also in ' + update_probes_file + '. Merging it.')
                    update_variable_number = update_probe_standard_names.index(variable_standard_name)
                    update_longitude = float(update_probe_longitudes[update_variable_number])
                    update_latitude = float(update_probe_latitudes[update_variable_number])
                    update_record_start = pd.to_datetime(update_probe_record_starts[update_variable_number])
                    update_record_end = pd.to_datetime(update_probe_record_ends[update_variable_number])
                    update_sampling_time = update_probe_sampling_times[update_variable_number]
                    try:
                        update_depths = np.array(update_probe_depths[update_variable_number], dtype=float)
                    except ValueError:
                        update_depths = update_probe_depths[update_variable_number]
                    update_quality_controls = update_probe_quality_controls[update_variable_number]
                    update_notes = update_probe_notes[update_variable_number]

                    merged_longitude = str(np.mean([base_longitude, update_longitude]))
                    merged_latitude = str(np.mean([base_latitude, update_latitude]))
                    merged_record_start =\
                        str(min(base_record_start, update_record_start))[:19]
                    merged_record_end = \
                        str(max(base_record_end, update_record_end))[:19]
                    if update_sampling_time != base_sampling_time:
                        merged_sampling_time = 'variable between datasets'
                    else:
                        merged_sampling_time = base_sampling_time
                    if base_is_dominant:
                        try:
                            merged_depths = ' '.join(np.array(base_depths, dtype=str))
                        except ValueError:
                            merged_depths = base_depths
                    elif (type(base_depths) is np.ndarray) and (type(update_depths) is np.ndarray):
                        merged_depths = ' '.join(np.array(np.sort(np.union1d(base_depths, update_depths)), dtype=str))
                    elif (base_depths is str) and (update_depths is str):
                        merged_depths = 'floating'
                    else:
                        time.sleep(sleep_time)
                        print(print_prefix + ' Error. Trying to merge a fixed with a floating probe. Exiting.',
                              file=sys.stderr)
                        time.sleep(sleep_time)
                        return

                    if (base_quality_controls == 'FULL') and (update_quality_controls == 'FULL'):
                        merged_quality_controls = 'FULL'
                    elif (base_quality_controls == 'INVERTED') and (update_quality_controls == 'INVERTED'):
                        merged_quality_controls = 'INVERTED'
                    elif (base_quality_controls == 'INVERTED') and (update_quality_controls == 'NO'):
                        merged_quality_controls = 'INVERTED'
                    elif (base_quality_controls == 'NO') and (update_quality_controls == 'NO'):
                        merged_quality_controls = 'NO'
                    else:
                        merged_quality_controls = 'PARTIAL'

                    merged_notes = ''
                    if 'renamed' in base_notes:
                        merged_notes += ' '.join(base_notes.split(' ')[0: 3]) + ' '

                    if ('duplicated and reversed records' in base_notes) or \
                            ('duplicated and reversed records' in update_notes):
                        merged_notes += 'duplicated and reversed records '
                    elif 'reversed records' in base_notes:
                        if 'duplicated records' in update_notes:
                            merged_notes += 'duplicated and reversed records '
                        else:
                            merged_notes += 'reversed records '
                    elif 'duplicated records' in base_notes:
                        if 'reversed records' in update_notes:
                            merged_notes += 'duplicated and reversed records '
                        else:
                            merged_notes += 'duplicated records '
                    elif 'reversed records' in update_notes:
                        merged_notes += 'reversed records '
                    elif 'duplicated records' in update_notes:
                        merged_notes += 'duplicated records '

                    if ('not averaged depth levels' in base_notes) or ('not averaged depth levels' in update_notes):
                        merged_notes += 'not averaged depth levels'
                    elif ('probably replaced sensors' in base_notes) or ('probably replaced sensors' in update_notes):
                        merged_notes += 'probably replaced sensors'
                else:
                    print(print_prefix + ' The variable standard name ' + variable_standard_name
                          + ' is not present in ' + update_probes_file + '. Copying it only.')
                    merged_longitude = str(base_longitude)
                    merged_latitude = str(base_latitude)
                    merged_record_start = str(base_record_start)[:19]
                    merged_record_end = str(base_record_end)[:19]
                    merged_sampling_time = base_sampling_time
                    try:
                        merged_depths = ' '.join(np.array(base_depths, dtype=str))
                    except ValueError:
                        merged_depths = base_depths
                    merged_quality_controls = base_quality_controls
                    merged_notes = base_notes

                merged_probe_standard_names.append(variable_standard_name)
                merged_probe_longitudes.append(merged_longitude)
                merged_probe_latitudes.append(merged_latitude)
                merged_probe_record_starts.append(merged_record_start)
                merged_probe_record_ends.append(merged_record_end)
                merged_probe_sampling_times.append(merged_sampling_time)
                merged_probe_depths.append(merged_depths)
                merged_probe_quality_controls.append(merged_quality_controls)
                merged_probe_notes.append(merged_notes)

            if not base_is_dominant:
                for update_variable_difference_number in range(len(update_standard_names_difference)):
                    variable_standard_name = update_standard_names_difference[update_variable_difference_number]
                    update_variable_number = update_probe_standard_names.index(variable_standard_name)
                    print(print_prefix + ' The variable standard name ' + variable_standard_name
                          + ' is not present in ' + update_probes_file + '. Copying it only.')
                    update_longitude = update_probe_longitudes[update_variable_number]
                    update_latitude = update_probe_latitudes[update_variable_number]
                    update_record_start = update_probe_record_starts[update_variable_number]
                    update_record_end = update_probe_record_ends[update_variable_number]
                    update_sampling_time = update_probe_sampling_times[update_variable_number]
                    update_quality_controls = update_probe_quality_controls[update_variable_number]
                    update_notes = update_probe_notes[update_variable_number]

                    merged_probe_standard_names.append(variable_standard_name)
                    merged_probe_longitudes.append(update_longitude)
                    merged_probe_latitudes.append(update_latitude)
                    merged_probe_record_starts.append(update_record_start)
                    merged_probe_record_ends.append(update_record_end)
                    merged_probe_sampling_times.append(update_sampling_time)
                    merged_probe_depths.append(' '.join(update_probe_depths[update_variable_number]))
                    merged_probe_quality_controls.append(update_quality_controls)
                    merged_probe_notes.append(update_notes)

        else:
            print(print_prefix + ' This probe is not present in CSV file ' + update_probes_file + '. Copying it only.')
            merged_probe_name = base_probe_name
            merged_probe_wmo = base_probe_wmo
            merged_probe_type = base_probe_type
            merged_probe_organization = base_probe_organization
            merged_probe_standard_names = base_probe_standard_names
            merged_probe_longitudes = base_probe_longitudes
            merged_probe_latitudes = base_probe_latitudes
            merged_probe_record_starts = base_probe_record_starts
            merged_probe_record_ends = base_probe_record_ends
            merged_probe_sampling_times = base_probe_sampling_times
            merged_probe_depths = [' '.join(depth) for depth in base_probe_depths]
            merged_probe_quality_controls = base_probe_quality_controls
            merged_probe_notes = base_probe_notes
            merged_probe_link = base_probe_link

        merged_probes_line = np.array([[probe_id, platform_code, merged_probe_name, merged_probe_wmo,
                                        merged_probe_type, merged_probe_organization,
                                        ';'.join(merged_probe_standard_names),
                                        ';'.join(merged_probe_longitudes), ';'.join(merged_probe_latitudes),
                                        ';'.join(merged_probe_record_starts), ';'.join(merged_probe_record_ends),
                                        ';'.join(merged_probe_sampling_times), ';'.join(merged_probe_depths),
                                        ';'.join(merged_probe_quality_controls), ';'.join(merged_probe_notes),
                                        merged_probe_link]],
                                      dtype=object)

        merged_probes_data = np.append(merged_probes_data, merged_probes_line, axis=0)
        # print(print_prefix + ' Writing merged probes CSV file...')
        # np.savetxt(out_merged_probes_file, merged_probes_data, fmt='"%s"', delimiter=',', comments='')

    if not base_is_dominant:
        print(' -------------------------')
        print(' Analyzing also update probes CSV file ' + update_probes_file)
        print(' -------------------------')
        print(' Copying probes that are only present in this file' + update_probes_file)
        update_probe_id = merged_probes_data.shape[0] - 1
        for platform_code in update_probes_data_difference:
            update_probe_id += 1
            print_prefix = '(' + platform_code + ')'
            update_probes_row = int(np.where(update_probes_platform_codes == platform_code)[0][0])
            update_probe_name = update_probes_names[update_probes_row]
            print(print_prefix + ' probe name = \'' + update_probe_name + '\'')
            print(print_prefix + ' Copying probe to merged CSV file.')
            merged_probe_wmo = update_probes_wmo[update_probes_row]
            merged_probe_type = update_probes_types[update_probes_row]
            merged_probe_organization = update_probes_organizations[update_probes_row]
            merged_probe_standard_names = update_probes_standard_names[update_probes_row]
            merged_probe_longitudes = update_probes_longitudes[update_probes_row]
            merged_probe_latitudes = update_probes_latitudes[update_probes_row]
            merged_probe_record_starts = update_probes_record_starts[update_probes_row]
            merged_probe_record_ends = update_probes_record_ends[update_probes_row]
            merged_probe_sampling_times = update_probes_sampling_times[update_probes_row]
            merged_probe_depths = [' '.join(depth) for depth in update_probes_depths[update_probes_row]]
            merged_probe_quality_controls = update_probes_quality_controls[update_probes_row]
            merged_probe_notes = update_probes_notes[update_probes_row]
            merged_probe_link = update_probes_links[update_probes_row]
            merged_probes_line = np.array([[update_probe_id, platform_code, update_probe_name, merged_probe_wmo,
                                            merged_probe_type, merged_probe_organization,
                                            ';'.join(merged_probe_standard_names),
                                            ';'.join(merged_probe_longitudes), ';'.join(merged_probe_latitudes),
                                            ';'.join(merged_probe_record_starts), ';'.join(merged_probe_record_ends),
                                            ';'.join(merged_probe_sampling_times), ';'.join(merged_probe_depths),
                                            ';'.join(merged_probe_quality_controls), ';'.join(merged_probe_notes),
                                            merged_probe_link]],
                                          dtype=object)
            merged_probes_data = np.append(merged_probes_data, merged_probes_line, axis=0)
            # print(print_prefix + ' Writing merged probes CSV file...')
            # np.savetxt(out_merged_probes_file, merged_probes_data, fmt='"%s"', delimiter=',', comments='')

    print(' -------------------------')
    print(' Building merged probes CSV file...')
    print(' -------------------------')
    out_merged_devices_ids = merged_devices_data[:, 0]
    out_merged_devices_types = merged_devices_data[:, 1]
    out_merged_organizations_ids = merged_organizations_data[:, 0]
    out_merged_organizations_names = merged_organizations_data[:, 1]
    out_merged_variables_ids = merged_variables_data[:, 0]
    out_merged_variables_standard_names = merged_variables_data[:, 1]
    for out_merged_probes_row in range(1, merged_probes_data.shape[0]):
        out_merged_probe_id = merged_probes_data[out_merged_probes_row, 0]
        out_merged_probe_platform_code = merged_probes_data[out_merged_probes_row, 1]
        print_prefix = '(' + out_merged_probe_platform_code + ')'
        out_merged_probe_name = merged_probes_data[out_merged_probes_row, 2]
        out_merged_probe_wmo = merged_probes_data[out_merged_probes_row, 3]
        out_merged_probe_type = merged_probes_data[out_merged_probes_row, 4]
        probe_device_type_index = np.where(out_merged_devices_types == out_merged_probe_type)[0][0]
        out_merged_probe_device_type_id = out_merged_devices_ids[probe_device_type_index]
        out_merged_probe_organization = merged_probes_data[out_merged_probes_row, 5]
        probe_organization_index = np.where(out_merged_organizations_names == out_merged_probe_organization)[0][0]
        out_merged_probe_organization_id = out_merged_organizations_ids[probe_organization_index]
        out_merged_probe_standard_names = merged_probes_data[out_merged_probes_row, 6].split(';')
        out_merged_probe_variables_ids = list()
        for standard_name in out_merged_probe_standard_names:
            probe_variable_index = np.where(out_merged_variables_standard_names == standard_name)[0][0]
            out_merged_probe_variables_ids.append(out_merged_variables_ids[probe_variable_index])
        out_merged_probe_variables_ids = ';'.join(out_merged_probe_variables_ids)
        out_merged_probe_longitudes = merged_probes_data[out_merged_probes_row, 7]
        out_merged_probe_latitudes = merged_probes_data[out_merged_probes_row, 8]
        out_merged_probe_record_starts = merged_probes_data[out_merged_probes_row, 9]
        out_merged_probe_record_ends = merged_probes_data[out_merged_probes_row, 10]
        out_merged_probe_sampling_times = merged_probes_data[out_merged_probes_row, 11]
        out_merged_probe_depths = merged_probes_data[out_merged_probes_row, 12]
        out_merged_probe_quality_controls = merged_probes_data[out_merged_probes_row, 13]
        out_merged_probe_notes = merged_probes_data[out_merged_probes_row, 14]
        out_merged_probe_link = merged_probes_data[out_merged_probes_row, 15]
        out_merged_probes_line = np.array([[out_merged_probe_id, out_merged_probe_platform_code, out_merged_probe_name,
                                            out_merged_probe_wmo, out_merged_probe_device_type_id,
                                            out_merged_probe_organization_id,
                                            out_merged_probe_variables_ids, out_merged_probe_longitudes,
                                            out_merged_probe_latitudes, out_merged_probe_record_starts,
                                            out_merged_probe_record_ends, out_merged_probe_sampling_times,
                                            out_merged_probe_depths, out_merged_probe_quality_controls,
                                            out_merged_probe_notes, out_merged_probe_link]], dtype=object)

        out_merged_probes_data = np.append(out_merged_probes_data, out_merged_probes_line, axis=0)
        print(print_prefix + ' Writing output merged probes CSV file...')
        np.savetxt(out_merged_probes_file, out_merged_probes_data, fmt='"%s"', delimiter=',', comments='')

    print(' -------------------------')
    total_run_time = time.gmtime(calendar.timegm(time.gmtime()) - start_run_time)
    print(' Finished! Total elapsed time is: '
          + str(int(np.floor(calendar.timegm(total_run_time) / 86400.))) + ' days '
          + time.strftime('%H:%M:%S', total_run_time) + ' hh:mm:ss')


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        base_csv_dir = sys.argv[1]
        update_csv_dir = sys.argv[2]
        merged_csv_dir = sys.argv[3]
    except (IndexError, ValueError):
        base_csv_dir = None
        update_csv_dir = None
        merged_csv_dir = None

    try:
        base_is_dominant = string_to_bool(sys.argv[4])
    except (IndexError, ValueError):
        base_is_dominant = True

    try:
        verbose = string_to_bool(sys.argv[5])
    except (IndexError, ValueError):
        verbose = True

    metadata_merger(base_csv_dir, update_csv_dir, merged_csv_dir, base_is_dominant, verbose)
