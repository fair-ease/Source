# -*- coding: utf-8 -*-
import sys
import os
import time
import numpy as np
import netCDF4
import calendar
from SOURCE import find_variable_name

# Global variables
sleep_time = 0.1  # seconds


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


def data_information_calc(in_file=None, in_variable_standard_name=None, valid_qc_values=None, in_depth_index=None,
                          first_date_str=None, last_date_str=None, verbose=True):
    if __name__ == '__main__':
        return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to calculate total, valid, invalid, no QC and filled data number for a depth sliced variable.')
        print(' -------------------------')
    if in_file is None or in_variable_standard_name is None or valid_qc_values is None or in_depth_index is None:
        time.sleep(sleep_time)
        print(' Error: 4 of 7 maximum arguments (3 optionals) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) input variable standard_name;', file=sys.stderr)
        print(' 3) input variable valid qc values to consider (spaced valued string, example: "0 1 2");',
              file=sys.stderr)
        print(' 4) input file depth index to check;', file=sys.stderr)
        print(' 5) (optional) first cut date in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: first recorded data);', file=sys.stderr)
        print(' 6) (optional) last cut date in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              '  default: last recorded data).', file=sys.stderr)
        print(' 7) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
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

    if verbose:
        print(' Input file = ' + in_file)
        print(' Input variable standard_name = ' + in_variable_standard_name)
        print(' Valid qc values to consider = ' + valid_qc_values)
        print(' Input depth slice index = ' + str(in_depth_index))
        print(' First date to process = ' + str(first_date_str) +
              ' (if None it will be the first available date)')
        print(' Last date to process = ' + str(last_date_str) +
              ' (if None it will be the last available date)')
        print(' Verbosity switch = ' + str(verbose))

    try:
        valid_qc_values = [int(valid_qc_value) for valid_qc_value in valid_qc_values.split(' ')]
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
        first_date_seconds = calendar.timegm(first_date)
    if last_date_str is not None:
        try:
            last_date = time.strptime(last_date_str, '%Y%m%d')
        except ValueError:
            last_date = time.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')
        last_date_seconds = calendar.timegm(last_date)

    in_data = netCDF4.Dataset(in_file, mode='r')

    # Loading record coordinate
    record_dimension = None
    for dimension in in_data.dimensions:
        if in_data.dimensions[dimension].isunlimited():
            record_dimension = dimension
            break
    in_time = in_data.variables[record_dimension]
    in_time_reference = in_time.units
    if 'days' in in_time_reference:
        in_time_data = np.round(in_time[...] * 86400.)
    elif 'seconds' in in_time_reference:
        in_time_data = np.round(in_time[...])
    in_time_reference = in_time_reference[in_time_reference.find('since ') + len('since '):]
    in_reference_data = abs(calendar.timegm(time.strptime(in_time_reference, '%Y-%m-%dT%H:%M:%SZ')))
    in_time_data += - in_reference_data

    if (first_date_str is not None) or (last_date_str is not None):
        if verbose:
            print(' Computing cutting indices...')
        if first_date_str is not None:
            out_time_mask = in_time_data >= first_date_seconds
        if last_date_str is not None:
            if first_date_str is not None:
                out_time_mask = np.logical_and(out_time_mask, in_time_data <= last_date_seconds)
            else:
                out_time_mask = in_time_data <= last_date_seconds
    else:
        out_time_mask = np.ones(in_time_data.shape[0], dtype=bool)

    if np.invert(out_time_mask).all():
        if verbose:
            time.sleep(sleep_time)
            print(' Warning: no data in the selected period for this variable.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
        return

    out_time_indices = np.where(out_time_mask)[0]

    total_records_number = len(out_time_indices)

    out_data_array = np.array([total_records_number], dtype=int)

    in_variable_name = find_variable_name.find_variable_name(in_file, 'standard_name', in_variable_standard_name,
                                                             verbose=False)

    in_variable_qc = in_data.variables[in_variable_name + '_qc']
    in_variable_qc_data = in_variable_qc[out_time_indices, in_depth_index]

    if np.ma.is_masked(in_variable_qc_data):
        in_variable_qc_data_fill_value = in_variable_qc_data.fill_value
        in_variable_qc_data = in_variable_qc_data.data
    else:
        in_variable_qc_data_fill_value = None

    try:
        flag_values = in_variable_qc.flag_values
        flag_meanings = in_variable_qc.flag_meanings.split(' ')
        no_qc_value_index = flag_meanings.index('no_qc_performed')
        no_qc_value = flag_values[no_qc_value_index]
    except (AttributeError, ValueError):
        try:
            flag_values = in_variable_qc.flag_values
            flag_meanings = in_variable_qc.flag_meanings.split(' ')
            no_qc_value_index = flag_meanings.index('no_quality_control')
            no_qc_value = flag_values[no_qc_value_index]
        except (AttributeError, ValueError):
            no_qc_value = False

    if no_qc_value:
        try:
            no_qc_index = np.where(valid_qc_values == no_qc_value)[0]
            valid_qc_values = np.delete(valid_qc_values, no_qc_index)
        except IndexError:
            pass
        no_qc_data_mask = in_variable_qc_data == no_qc_value
        no_qc_data = in_variable_qc_data[no_qc_data_mask]
        no_qc_values_number = no_qc_data.shape[0]
        no_qc_indices = np.where(no_qc_data_mask)[0]
        try:
            last_no_qc_index = no_qc_indices[-1]
        except IndexError:
            last_no_qc_index = -1
    else:
        no_qc_values_number = 0
        last_no_qc_index = -1

    out_data_array = np.append(out_data_array, [no_qc_values_number, last_no_qc_index])

    if no_qc_value:
        if verbose:
            print(' Total number of no qc values for field ' + in_variable_standard_name + ' :')
            print('     ' + str(no_qc_values_number) + ' / ' + str(in_variable_qc_data.shape[0]))

    valid_data_mask = np.zeros(in_variable_qc_data.shape, dtype=bool)
    invalid_data_mask = np.ones(in_variable_qc_data.shape, dtype=bool)
    for valid_qc_value in valid_qc_values:
        valid_data_mask = np.logical_or(valid_data_mask, in_variable_qc_data == valid_qc_value)
        invalid_data_mask = np.logical_and(invalid_data_mask, in_variable_qc_data != valid_qc_value)
    valid_data = in_variable_qc_data[valid_data_mask]
    valid_values_number = valid_data.shape[0]
    valid_indices = np.where(valid_data_mask)[0]
    try:
        last_valid_index = valid_indices[-1]
    except IndexError:
        last_valid_index = -1

    out_data_array = np.append(out_data_array, [valid_values_number, last_valid_index])

    if verbose:
        print(' Total number of valid values for field ' + in_variable_standard_name + ' :')
        print('     ' + str(valid_values_number) + ' / ' + str(in_variable_qc_data.shape[0]))

    if in_variable_qc_data_fill_value is not None:
        filled_data_mask = in_variable_qc_data == in_variable_qc_data_fill_value
        invalid_data_mask = np.logical_and(invalid_data_mask, in_variable_qc_data != in_variable_qc_data_fill_value)
    else:
        filled_data_mask = np.zeros(in_variable_qc_data.shape, dtype=bool)
    try:
        flag_values = in_variable_qc.flag_values
        flag_meanings = in_variable_qc.flag_meanings.split(' ')
        missing_value_index = flag_meanings.index('missing_value')
        missing_value = flag_values[missing_value_index]
        filled_data_mask = np.logical_or(filled_data_mask, in_variable_qc_data == missing_value)
        invalid_data_mask = np.logical_and(invalid_data_mask, in_variable_qc_data != missing_value)
    except (AttributeError, IndexError, ValueError):
        pass

    filled_data = in_variable_qc_data[filled_data_mask]
    filled_values_number = filled_data.shape[0]
    filled_indices = np.where(filled_data_mask)[0]
    try:
        last_filled_index = filled_indices[-1]
    except IndexError:
        last_filled_index = -1

    out_data_array = np.append(out_data_array, [filled_values_number, last_filled_index])

    if verbose:
        print(' Total number of filled values for field ' + in_variable_standard_name + ' :')
        print('     ' + str(filled_values_number) + ' / ' + str(in_variable_qc_data.shape[0]))

    invalid_data = in_variable_qc_data[invalid_data_mask]
    invalid_values_number = invalid_data.shape[0]
    invalid_indices = np.where(invalid_data_mask)[0]
    try:
        last_invalid_index = invalid_indices[-1]
    except IndexError:
        last_invalid_index = -1

    out_data_array = np.append(out_data_array, [invalid_values_number, last_invalid_index])

    if verbose:
        print(' Total number of invalid values for field ' + in_variable_standard_name + ' :')
        print('     ' + str(invalid_data.shape[0]) + ' / ' + str(in_variable_qc_data.shape[0]))

        print(' -------------------------')

    in_data.close()
    return out_data_array


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_file = sys.argv[1]
        in_variable_standard_name = sys.argv[2]
        valid_qc_values = sys.argv[3]
        in_depth_index = int(sys.argv[4])

    except (IndexError, ValueError):
        in_file = None
        in_variable_standard_name = None
        valid_qc_values = None
        in_depth_index = None

    try:
        time.strptime(sys.argv[5], '%Y%m%d')
        first_date_str = sys.argv[5]
    except (IndexError, ValueError):
        try:
            time.strptime(sys.argv[5], '%Y-%m-%d %H:%M:%S')
            first_date_str = sys.argv[5]
        except (IndexError, ValueError):
            first_date_str = None

    try:
        time.strptime(sys.argv[6], '%Y%m%d')
        last_date_str = sys.argv[6]
    except (IndexError, ValueError):
        try:
            time.strptime(sys.argv[6], '%Y-%m-%d %H:%M:%S')
            last_date_str = sys.argv[6]
        except (IndexError, ValueError):
            last_date_str = None

    try:
        verbose = string_to_bool(sys.argv[7])
    except (IndexError, ValueError):
        verbose = True

    out_data_array = data_information_calc(in_file, in_variable_standard_name, valid_qc_values, in_depth_index,
                                           first_date_str, last_date_str, verbose)
    out_data_list = map(str, out_data_array)
    print(' '.join(out_data_list))
