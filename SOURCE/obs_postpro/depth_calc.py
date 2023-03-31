# -*- coding: utf-8 -*-
import sys
import os
import time
import numpy as np
import netCDF4

# Global variables
sleep_time = 0.1  # seconds
out_fill_value = 1.e20
depth_minimum_space = 0.5  # m
depth_average_threshold = 5 / 100  # %
filled_data_threshold = 1 / 100  # %
threshold_tolerance = 20 / 100  # %


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def depth_calc(in_file=None, in_variable_standard_name=None, verbose=True):
    # if __name__ == '__main__':
    #     return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to calculate rounded depth slice array for processed observation files.')
        print(' -------------------------')
    if in_file is None:
        time.sleep(sleep_time)
        print(' Error: 1 of 3 maximum arguments (1 optional) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) (optional) input variable standard_name (defalut: None);', file=sys.stderr)
        print(' 3) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    if verbose:
        print(' Input file = ' + in_file)
        print(' Input variable standard_name = ' + str(in_variable_standard_name))
        print(' Verbosity switch = ' + str(verbose))
        print(' -------------------------')

    in_data = netCDF4.Dataset(in_file, mode='r')
    # Retrieve depth variable
    try:
        in_depth = in_data.variables['depth']
        in_depth_data = np.round(in_depth[...], decimals=5)
    except KeyError:
        # Is only surface dataset
        return [np.array([True, True, True, True]), np.array([0.])]
    if not np.ma.is_masked(in_depth_data):
        in_depth_data = np.ma.array(in_depth_data,
                                    mask=np.zeros(shape=in_depth_data.shape, dtype=bool),
                                    fill_value=out_fill_value, dtype=in_depth_data.dtype)

    depth_is_constant = True
    if len(in_depth_data.shape) > 1:
        for depth in range(in_depth_data.shape[1]):
            unique_depth_slice = np.unique(in_depth_data[:, depth])
            if unique_depth_slice.shape[0] > 1:
                depth_is_constant = False
                break

    if verbose:
        print(' Depth is constant switch = ' + str(depth_is_constant))

    unique_depths, unique_depths_counts = np.unique(in_depth_data.data, return_counts=True)
    unique_depths_counts = unique_depths_counts[np.abs(unique_depths - out_fill_value) > out_fill_value / 10]
    unique_depths = unique_depths[np.abs(unique_depths - out_fill_value) > out_fill_value / 10]

    if verbose and (unique_depths.shape[0] < 50):
        print(' Unique depth levels:')
        print(' ' + ' '.join(map(str, unique_depths)) + ' meters.')
    elif verbose:
        print(' Unique depth levels shape too big to print. Shape is: ' + str(unique_depths.shape[0]))

    depth_is_good_spaced = True
    for depth in range(1, unique_depths.size):
        if (np.abs(unique_depths[depth] - unique_depths[depth - 1]) <
            unique_depths[depth - 1] * depth_average_threshold) or \
                (np.abs(unique_depths[depth] - unique_depths[depth - 1]) < depth_minimum_space):
            depth_is_good_spaced = False
            break

    if verbose:
        print(' Depth is good spaced switch = ' + str(depth_is_good_spaced))

    if depth_is_good_spaced:
        unique_rounded_depths = unique_depths
    else:
        if verbose:
            print(' Rounding input depth levels to a '
                  + str(int(depth_average_threshold * 100)) + '% minimum increment.')
        # Compute rounded and unique depths data array
        unique_rounded_depths = np.empty(shape=0, dtype=in_depth.dtype)
        depth = 0
        while depth < unique_depths.size - 1:
            for depth_bis in range(depth + 1, unique_depths.size):
                rounded_depth = np.average(unique_depths[depth: depth_bis],
                                           weights=unique_depths_counts[depth: depth_bis])
                if (np.abs(unique_depths[depth_bis] - unique_depths[depth]) >=
                        np.max([rounded_depth * depth_average_threshold, depth_minimum_space])):
                    break
            unique_rounded_depths = np.append(unique_rounded_depths, rounded_depth)
            depth = depth_bis
    if verbose:
        if not unique_rounded_depths.shape[0] == unique_depths.shape[0]:
            print(' Rounded depth levels:')
            print(' ' + ' '.join(map(str, unique_rounded_depths)) + ' meters.')
        elif not np.allclose(unique_rounded_depths, unique_depths):
            print(' Rounded depth levels:')
            print(' ' + ' '.join(map(str, unique_rounded_depths)) + ' meters.')

    check_variables = [variable for variable in in_data.variables.keys()
                       if 'time' in in_data.variables[variable].dimensions]
    check_variables = [variable for variable in check_variables if variable not in ['lon', 'lat', 'depth', 'time']]
    
    good_data_depth_levels = True
    double_break = False
    for variable_name in check_variables:
        if double_break:
            break
        in_variable = in_data.variables[variable_name]
        work_variable_data = in_variable[...]
        if not np.ma.is_masked(work_variable_data):
            work_variable_data = np.ma.array(work_variable_data,
                                             mask=np.zeros(shape=work_variable_data.shape, dtype=bool),
                                             fill_value=out_fill_value, dtype=float)
        for depth in unique_rounded_depths:
            indices = np.abs(in_depth_data - depth) <= \
                      np.max([np.abs(depth) * depth_average_threshold, depth_minimum_space]) * \
                      (1 + threshold_tolerance)
            if np.any(indices):
                temp_variable = np.ma.masked_all(shape=work_variable_data.shape, dtype=in_variable.datatype)
                temp_variable = np.ma.where(indices, work_variable_data, temp_variable)
                mean_variable = np.ma.mean(temp_variable, axis=1)
                work_variable_data = np.ma.where(indices, out_fill_value, work_variable_data)
                if len(in_depth_data.shape) > 1:
                    work_variable_data[indices] = out_fill_value
                    try:
                        work_variable_data.mask[indices] = True
                    except TypeError:
                        pass
                else:
                    work_variable_data[:, indices] = out_fill_value
                    try:
                        work_variable_data.mask[:, indices] = True
                    except TypeError:
                        pass
                not_filled_data_number = mean_variable.shape[0] - np.ma.count_masked(mean_variable)
                if not_filled_data_number < mean_variable.shape[0] * filled_data_threshold:
                    good_data_depth_levels = False
                    double_break = True
                    break
            else:
                good_data_depth_levels = False
                double_break = True
                break
                    
    if verbose:
        print(' Good data depth levels switch = ' + str(good_data_depth_levels))

    depth_is_positive = True

    for depth in range(unique_rounded_depths.shape[0]):
        if unique_rounded_depths[depth] < 0:
            depth_is_positive = False
            break

    if verbose:
        print(' Depth is positive switch = ' + str(depth_is_positive))

    not_filled_data_numbers = np.zeros(shape=unique_rounded_depths.shape[0])
    not_filled_data_names = [''] * unique_rounded_depths.shape[0]
    for variable_name in check_variables:
        in_variable = in_data.variables[variable_name]
        try:
            work_variable_data = in_variable[:, :, 0, 0]
        except ValueError:
            work_variable_data = in_variable[...]
        if not np.ma.is_masked(work_variable_data):
            work_variable_data = np.ma.array(work_variable_data,
                                             mask=np.zeros(shape=work_variable_data.shape, dtype=bool),
                                             fill_value=out_fill_value, dtype=float)
        for depth in range(unique_rounded_depths.shape[0]):
            indices = np.abs(in_depth_data - unique_rounded_depths[depth]) <= \
                      np.max([np.abs(unique_rounded_depths[depth]) * depth_average_threshold, depth_minimum_space]) * \
                      (1 + threshold_tolerance)
            if np.any(indices):
                temp_variable = np.ma.masked_all(shape=work_variable_data.shape, dtype=in_variable.datatype)
                temp_variable = np.ma.where(indices, work_variable_data, temp_variable)
                mean_variable = np.ma.mean(temp_variable, axis=1)
                if len(in_depth_data.shape) > 1:
                    work_variable_data[indices] = out_fill_value
                    work_variable_data.mask[indices] = True
                else:
                    work_variable_data[:, indices] = out_fill_value
                    work_variable_data.mask[:, indices] = True
                if not_filled_data_numbers[depth] < mean_variable.shape[0] - np.ma.count_masked(mean_variable):
                    not_filled_data_numbers[depth] = mean_variable.shape[0] - np.ma.count_masked(mean_variable)
                    not_filled_data_names[depth] = variable_name
    if verbose:
        print(' -------------------------')
        print(' Data information:')
    for depth in range(unique_rounded_depths.shape[0]):
        if verbose:
            print(' ' + str(unique_rounded_depths[depth]) + ' meters: ' + str(int(not_filled_data_numbers[depth])) +
                  ', ' + str(int(not_filled_data_numbers[depth] * 100 / mean_variable.shape[0])) +
                  '% of good data (variable with the most data quantity is '
                  + not_filled_data_names[depth] + ').')
    if verbose:
        print(' -------------------------')

    if good_data_depth_levels:
        good_depth_data = unique_rounded_depths
    else:
        if verbose:
            print(' Removing depth levels with less ' +
                  str(int(filled_data_threshold * 100)) + '% of data for every variable.')
        good_depth_data = np.empty(shape=0, dtype=in_depth.dtype)
        for depth in range(unique_rounded_depths.shape[0]):
            depth_value = unique_rounded_depths[depth]
            if not_filled_data_numbers[depth] >= mean_variable.shape[0] * filled_data_threshold:
                good_depth_data = np.ma.append(good_depth_data, depth_value)
            else:
                continue

    if verbose:
        if not good_depth_data.shape[0] == unique_rounded_depths.shape[0]:
            print(' Good depth levels:')
            print(' ' + ' '.join(map(str, good_depth_data)) + ' meters.')
        elif not np.allclose(good_depth_data, unique_rounded_depths):
            print(' Good depth levels:')
            print(' ' + ' '.join(map(str, good_depth_data)) + ' meters.')

    if in_variable_standard_name is not None:
        if ('water' in in_variable_standard_name) or ('surface' in in_variable_standard_name):
            if verbose:
                print(' Sea water variable detected. Finding and removing negative depth levels...')
            if not depth_is_positive:
                positive_depth_data = np.empty(shape=0, dtype=in_depth.datatype)
                for depth in range(good_depth_data.shape[0]):
                    if good_depth_data[depth] < 0.:
                        continue
                    else:
                        positive_depth_data = np.append(positive_depth_data, good_depth_data[depth])
            else:
                positive_depth_data = good_depth_data
        else:
            positive_depth_data = good_depth_data
    else:
        positive_depth_data = good_depth_data

    if verbose and not np.allclose(positive_depth_data, good_depth_data):
        print(' Positive depth levels:')
        print(' ' + ' '.join(map(str, positive_depth_data)) + ' meters.')

    if verbose:
        print(' Dataset information:')
        if depth_is_constant:
            print(' 1) Depth field is constant in time;')
        else:
            print(' 1) Depth field is variable in time;')
        if depth_is_good_spaced:
            print(' 2) Depth dimension is good spaced (i.e. from every point to the successor there is at least '
                  + str(depth_minimum_space) + 'm and a minimum of '
                  + str(int(depth_average_threshold * 100)) + '% increment)')
        else:
            print(' 2) Depth dimension is not good spaced (i.e. from every point to the successor there not ever '
                  'at least ' + str(depth_minimum_space) + 'm or a minimum of '
                  + str(int(depth_average_threshold * 100)) + '% increment)')
        if good_data_depth_levels:
            print(' 3) Variables have all depth levels with at least ' +
                  str(int(filled_data_threshold * 100)) + '% of good data.')
        else:
            print(' 3) There are some depth levels in which all variables have less of ' +
                  str(int(filled_data_threshold * 100)) + '% of good data.')
            print(' -------------------------')
        if depth_is_positive:
            print(' 4) Depth variable is positive.')
        else:
            print(' 4) There are some negative depth values.')
            print(' -------------------------')

    out_depth_data = positive_depth_data

    in_data.close()

    depth_information_array = np.array([depth_is_constant, depth_is_good_spaced,
                                        good_data_depth_levels, depth_is_positive])

    return [depth_information_array, np.round(out_depth_data, decimals=1)]


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_file = sys.argv[1]

    except (IndexError, ValueError):
        in_file = None

    try:
        in_variable_standard_name = sys.argv[2]
    except (IndexError, ValueError):
        in_variable_standard_name = None

    try:
        verbose = string_to_bool(sys.argv[3])
    except (IndexError, ValueError):
        verbose = True

    [depth_information_array, out_depth_data] = depth_calc(in_file, in_variable_standard_name, verbose)
    out_depth_data_list = map(str, out_depth_data)
    print(' '.join(out_depth_data_list))
