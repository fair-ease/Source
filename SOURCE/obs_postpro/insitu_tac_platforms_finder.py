# -*- coding: utf-8 -*-
import sys
import os
import numpy as np
import netCDF4
import time
import calendar
import unidecode

# Global variables
sleep_time = 0.1  # seconds
mean_duplicate_threshold = 0.09  # degrees
# To prevent latitude-longitude error accumulation in average process
variance_duplicate_threshold = 1  # degrees


def longest_common_substring(string_1, string_2):
    # noinspection PyUnusedLocal
    index = [[0] * (1 + len(string_2)) for i in range(1 + len(string_1))]
    longest = 0
    x_longest = 0
    for character_1 in range(1, 1 + len(string_1)):
        for character_2 in range(1, 1 + len(string_2)):
            if string_1[character_1 - 1] == string_2[character_2 - 1]:
                index[character_1][character_2] = index[character_1 - 1][character_2 - 1] + 1
                if index[character_1][character_2] > longest:
                    longest = index[character_1][character_2]
                    x_longest = character_1
            else:
                index[character_1][character_2] = 0
    return string_1[x_longest - longest: x_longest]


def string_intersection(string_1, string_2):
    out_string = ""
    if len(string_1) >= len(string_2):
        short_string = string_2
        long_string = string_1
    else:
        short_string = string_1
        long_string = string_2
    for character in short_string:
        if character in long_string:
            out_string += character
    return out_string


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def insitu_tac_platforms_finder(in_list=None, longitude_mean=None, latitude_mean=None,
                                in_fields_standard_name_str=None,
                                first_date_str=None, last_date_str=None, verbose=True):
    # if __name__ == '__main__':
    #     return
    if verbose:
        print('-------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Local time is ' + time.strftime("%a %b %d %H:%M:%S %Z %Y", time.gmtime()))
        print(' -------------------------')
        print(' CMEMS IN SITU TAC surrounding datasets finder.')
        print(' -------------------------')
    if in_list is None or longitude_mean is None or latitude_mean is None:
        time.sleep(sleep_time)
        print(' ERROR: 3 of 7 maximum arguments (4 optionals) not provided.', file=sys.stderr)
        print(' 1) Input datasets list;', file=sys.stderr)
        print(' 2) average longitude for surrounding search;', file=sys.stderr)
        print(' 3) average latitude for surrounding search;', file=sys.stderr)
        print(' 4) (optional) Input fields standard_name space separated string to process'
              ' (for example: "sea_water_temperature sea_water_practical_salinity");', file=sys.stderr)
        print(' 5) (optional) First date to evaluate in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: first recorded date for each device);', file=sys.stderr)
        print(' 6) (optional) Last date to evaluate in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: last recorded date for each device);', file=sys.stderr)
        print(' 7) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    try:
        in_list = in_list.split(' ')
    except AttributeError:
        pass
    file_list = [element for element in in_list if element.endswith('.nc')]
    if not file_list:
        file_list = [in_list[0] + '/' + in_file for in_file in os.listdir(in_list[0]) if in_file.endswith('.nc')]
        if not file_list:
            time.sleep(sleep_time)
            print(' Error. No processable files in input directory.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return
    if verbose:
        print(' Input list = ' + ', '.join(file_list))
        print(' average longitude = ' + str(longitude_mean))
        print(' average latitude = ' + str(latitude_mean))
        print(' Input variables presence to check standard_name string = ' + in_fields_standard_name_str +
              ' (if None no check will be computed)')
        print(' First date to process = ' + str(first_date_str) +
              ' (if None it will be the first available date on each dataset)')
        print(' Last date to process = ' + str(last_date_str) +
              ' (if None it will be the last available date on each dataset)')
        print(' verbosity switch = ' + str(verbose))
        print(' -------------------------')
        print(' Starting process...')
        print(' -------------------------')

    if (in_fields_standard_name_str is None) or (in_fields_standard_name_str == 'None') or \
            (in_fields_standard_name_str == '') or len(in_fields_standard_name_str.split(' ')) < 1:
        in_fields_standard_name_list = None
    else:
        in_fields_standard_name_list = in_fields_standard_name_str.split(' ')

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

    if first_date_str is not None:
        first_date_seconds = calendar.timegm(first_date)
    if last_date_str is not None:
        last_date_seconds = calendar.timegm(last_date)

    found_list = list()
    if verbose:
        print(' Searching all surroundings datasets matching this platform_code...')
    first_file = True
    out_platform_code = ''
    for in_file in file_list:
        in_data = netCDF4.Dataset(in_file, mode='r')
        in_platform_code = in_data.platform_code
        try:
            in_probe_name = in_data.platform_name
        except AttributeError:
            in_probe_name = ''
        try:
            in_probe_wmo = in_data.wmo_platform_code
        except AttributeError:
            in_probe_wmo = ''
        if in_probe_wmo == ' ':
            in_probe_wmo = ''
        try:
            in_probe_type = in_data.source
        except AttributeError:
            in_probe_type = ''
        if (in_probe_type == '') or (in_probe_type == ' '):
            in_probe_type = 'undefined'
        try:
            in_probe_organization = unidecode.unidecode(in_data.institution.replace(',', ''))
        except AttributeError:
            in_probe_organization = ''
        if in_probe_organization == ' ':
            in_probe_organization = ''
        try:
            in_probe_link = in_data.references
        except AttributeError:
            in_probe_link = ''
        if in_probe_link == ' ':
            in_probe_link = ''
        in_data_standard_names = list()
        for variable in in_data.variables.keys():
            try:
                variable_standard_name = in_data.variables[variable].standard_name
                in_data_standard_names.append(variable_standard_name)
            except AttributeError:
                continue
            if variable_standard_name == 'longitude':
                in_longitude_name = variable
            if variable_standard_name == 'latitude':
                in_latitude_name = variable
            if variable_standard_name == 'time':
                in_time_name = variable
        in_time_reference = in_data.variables[in_time_name].units
        if 'days' in in_time_reference:
            in_time_data = np.round(in_data.variables[in_time_name][...] * 86400.)
        elif 'seconds' in in_time_reference:
            in_time_data = np.round(in_data.variables[in_time_name][...])

        in_longitude_mean = np.around(np.mean(in_data.variables[in_longitude_name][...]), decimals=3)
        in_longitude_variance = np.around(np.var(in_data.variables[in_longitude_name][...]), decimals=3)
        in_latitude_mean = np.around(np.mean(in_data.variables[in_latitude_name][...]), decimals=3)
        in_latitude_variance = np.around(np.var(in_data.variables[in_latitude_name][...]), decimals=3)

        in_data.close()
        list_intersection =\
            [standard_name for standard_name in in_fields_standard_name_list if standard_name in in_data_standard_names]
        if not list_intersection:
            continue
        in_time_reference = in_time_reference[in_time_reference.find('since ') + len('since '):]
        try:
            in_reference_data = abs(calendar.timegm(time.strptime(in_time_reference, '%Y-%m-%dT%H:%M:%SZ')))
        except ValueError:
            in_reference_data = abs(calendar.timegm(time.strptime(in_time_reference, '%Y-%m-%d %H:%M:%S')))
        in_time_data += - in_reference_data

        start_time = np.min(in_time_data)
        if not first_file:
            previous_end_time = end_time
        end_time = np.max(in_time_data)

        if first_date_str is not None:
            if end_time < first_date_seconds:
                continue
        if last_date_str is not None:
            if start_time > last_date_seconds:
                continue

        horizontal_condition = \
            (np.abs(in_longitude_mean - longitude_mean) < mean_duplicate_threshold) and \
            (in_longitude_variance < variance_duplicate_threshold) and \
            (np.abs(in_latitude_mean - latitude_mean) < mean_duplicate_threshold) and \
            (in_latitude_variance < variance_duplicate_threshold)
        if first_file:
            platform_code_condition = True
            out_platform_code = in_platform_code
            out_probe_name = in_probe_name
            out_probe_wmo = in_probe_wmo
            out_probe_type = in_probe_type
            out_probe_organization = in_probe_organization
            out_probe_link = in_probe_link
        else:
            platform_code_intersection = \
                longest_common_substring(in_platform_code.lower(), out_platform_code.lower())
            platform_code_intersection = platform_code_intersection.replace(' ', '').replace('-', '').replace('_', '')
            platform_code_condition = \
                len(platform_code_intersection) > 2 or (in_platform_code.lower() == out_platform_code.lower())
        condition = horizontal_condition and platform_code_condition
        if condition:
            found_list.append(in_file)
            if first_file:
                first_file = False
            else:
                if end_time > previous_end_time:
                    out_platform_code = in_platform_code
                if (out_probe_name == '') or \
                        (out_probe_name == ' ') or \
                        (out_probe_name == 'undefined') or \
                        (out_probe_name == 'unknown'):
                    out_probe_name = in_probe_name
                elif end_time > previous_end_time:
                    out_probe_name = in_probe_name
                if (out_probe_wmo == '') or \
                        (out_probe_wmo == ' ') or \
                        (out_probe_wmo == 'undefined') or \
                        (out_probe_wmo == 'unknown'):
                    out_probe_wmo = in_probe_wmo
                elif end_time > previous_end_time:
                    out_probe_wmo = in_probe_wmo
                if (out_probe_type == '') or \
                        (out_probe_type == ' ') or \
                        (out_probe_type == 'undefined') or \
                        (out_probe_type == 'unknown'):
                    out_probe_type = in_probe_type
                elif end_time > previous_end_time:
                    out_probe_type = in_probe_type
                if (out_probe_organization == '') or \
                        (out_probe_organization == ' ') or \
                        (out_probe_organization == 'undefined') or \
                        (out_probe_organization == 'unknown'):
                    out_probe_organization = in_probe_organization
                elif end_time > previous_end_time:
                    out_probe_organization = in_probe_organization
                if (out_probe_link == '') or \
                        (out_probe_link == ' ') or \
                        (out_probe_link == 'undefined') or \
                        (out_probe_link == 'unknown'):
                    out_probe_link = in_probe_link
                elif end_time > previous_end_time:
                    out_probe_link = in_probe_link

    if not found_list:
        if verbose:
            time.sleep(sleep_time)
            print(' Warning: all datasets are out of the specified range. Exiting.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
        return [], '', '', '', '', '', ''
    if verbose:
        print(' Found datasets:')
        for file_number in range(len(found_list)):
            print(' ' + str(file_number + 1) + ') ' + os.path.basename(found_list[file_number]))

    return found_list, out_platform_code, out_probe_name, \
        out_probe_wmo, out_probe_type, out_probe_organization, out_probe_link


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_list = sys.argv[1]
        longitude_mean = sys.argv[2]
        latitude_mean = sys.argv[3]
    except (IndexError, ValueError):
        in_list = None
        longitude_mean = None
        latitude_mean = None

    try:        
        in_fields_standard_name_str = sys.argv[4]
    except (IndexError, ValueError):
        in_fields_standard_name_str = None

    try:
        first_date_str = sys.argv[5]
    except (IndexError, ValueError):
        first_date_str = None

    try:
        last_date_str = sys.argv[6]
    except (IndexError, ValueError):
        last_date_str = None

    try:
        verbose = string_to_bool(sys.argv[7])
    except (IndexError, ValueError):
        verbose = True

    insitu_tac_platforms_finder(in_list, longitude_mean, latitude_mean,
                                in_fields_standard_name_str, first_date_str, last_date_str, verbose)
