# -*- coding: utf-8 -*-
import sys
import os
import numpy as np
from sklearn.neighbors import KernelDensity
from scipy import interpolate
from sklearn.linear_model import LinearRegression
import netCDF4
import pandas as pd
import time
import calendar
from SOURCE import find_variable_name, time_calc

plotting = False

if plotting:
    import os
    import matplotlib
    matplotlib.use('TkAgg')
    import matplotlib.pyplot as plt


# Global variables
sleep_time = 0.1  # seconds
out_fill_value = 1.e20
# Range check
range_check_enabled = True
# Spike test
spike_test_enabled = True
spiked_neighborhood_test_points = 3
spiked_range_multiplier = 2
# Stuck value_test
stuck_value_test_enabled = True
stuck_minimum_count = 100
stuck_neighborhood_test_points = 100
stuck_values_multiplier = 5
# Statistic QC iterations
statistic_probability_threshold = 5 / 100
valid_data_minimum_days = 15  # For blocking statistic monthly part
# delta_x for density computation
delta_x = 0.1
# Isolated points test
isolated_points_test_enabled = False
neighborhood_time_steps_threshold = 15  # time steps
neighborhood_days_threshold = 15  # days


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def time_series_post_processing(in_file=None, in_variable_standard_name=None, update_mode=None,
                                routine_qc_iterations=None, climatology_file=None,
                                out_file=None, verbose=True):
    if __name__ == '__main__':
        return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to reprocess a field in insitu observations netCDF files.')
        print(' -------------------------')
    if in_file is None or in_variable_standard_name is None or update_mode is None or \
            routine_qc_iterations is None or climatology_file is None or out_file is None:
        time.sleep(sleep_time)
        print(' Error: 6 of 7 maximum arguments (1 optional) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) input variable standard_name;', file=sys.stderr)
        print(' 3) update mode execution switch (True or False) (default: False);', file=sys.stderr)
        print(' 4) Statistic quality check iterations number (N, integer), options:', file=sys.stderr)
        print('     b) N = 0 for gross check quality controls only;', file=sys.stderr)
        print('     c) N >= 1 for N statistic quality check iterations;', file=sys.stderr)
        print('     note: if update mode is selected, any value selected above 1 will be ignored',
              file=sys.stderr)
        print(' 5) historical climatology file (input in update mode, output in creation mode,' +
              ' ignored if iterations number is equal to 0);',
              file=sys.stderr)
        print(' 6) output file;', file=sys.stderr)
        print(' 7) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    if verbose:
        print(' Input file = ' + in_file)
        print(' Input variable standard_name = ' + in_variable_standard_name)
        print(' Update mode = ' + str(update_mode))
        print(' Routine quality check iterations number = ' + str(routine_qc_iterations))
        print(' Climatology file = ' + climatology_file)
        print(' Output file = ' + out_file)
        print(' Verbosity switch = ' + str(verbose))
        print(' Starting process...')
        print(' -------------------------')

        print(' Opening input dataset.')

    # Enable or disable slight time rounding by two units down from the sampling time
    time_rounding = True

    # Open input dataset
    in_data = netCDF4.Dataset(in_file, mode='r')

    # Loading record coordinate
    in_time = in_data.variables['time']
    in_time_reference = in_time.units
    if 'days' in in_time_reference:
        in_time_data = np.round(in_time[...] * 86400.)
    elif 'seconds' in in_time_reference:
        in_time_data = np.round(in_time[...])

    sampling_time_seconds = time_calc.time_calc(in_file, verbose=False)
    if verbose:
        sampling_time = time.strftime('%H:%M:%S', time.gmtime(sampling_time_seconds))
        print(' most representative sampling time: ' + sampling_time + ' HH:MM:SS')

    if time_rounding:
        if sampling_time_seconds < 29:
            round_frequency = '10ms'
        elif sampling_time_seconds < 60:
            round_frequency = '100ms'
        elif sampling_time_seconds < 10 * 60:
            round_frequency = 's'
        elif sampling_time_seconds < 3600:
            round_frequency = '10s'
        elif sampling_time_seconds < 10 * 3600:
            round_frequency = 'min'
        elif sampling_time_seconds < 86400:
            round_frequency = '10min'
        elif sampling_time_seconds < 10 * 86400:
            round_frequency = 'H'
        elif sampling_time_seconds < 31 * 86400:
            round_frequency = '10H'
        else:
            round_frequency = 'D'
        in_time_series = pd.Series(in_time_data * 1.e9, dtype='datetime64[ns]')
        rounded_time_series = in_time_series.dt.round(round_frequency)
        if pd.Series.equals(in_time_series, rounded_time_series):
            time_rounding = False
    if time_rounding:
        if verbose:
            print(' Slight rounding time variable by two units down from the sampling time.')
        [out_time_series, unique_indices] = \
            np.unique(rounded_time_series, return_index=True)
        duplicated_indices = np.unique(list(set(np.arange(in_time_series.shape[0])) - set(unique_indices)))
        if len(duplicated_indices) > 0:
            delete_mask = np.ones(len(in_time_series), dtype=bool)
            delete_mask[duplicated_indices] = False
        out_time_series = pd.to_datetime(out_time_series)
    else:
        out_time_series = pd.to_datetime(in_time_data * 1.e9)
    out_time_data = out_time_series.astype(np.int64).values / 1.e9

    try:
        in_depth = in_data.variables['depth']
        in_depth_data = in_depth[...]
    except KeyError:
        if 'time' in in_data.variables['lon'].dimensions:
            in_depth_data = np.zeros(shape=(out_time_series.shape[0], 1))
        else:
            in_depth_data = 0.
    if np.ma.is_masked(in_depth_data):
        in_depth_data = np.ma.array(np.where(in_depth_data.data == in_depth_data.fill_value,
                                             out_fill_value, in_depth_data),
                                    mask=in_depth_data.mask, fill_value=out_fill_value, dtype=in_depth_data.dtype)
    else:
        in_depth_data = np.ma.array(in_depth_data, mask=np.zeros(shape=in_depth_data.shape, dtype=bool),
                                    fill_value=out_fill_value, dtype=in_depth_data.dtype)

    in_variable_name = find_variable_name.find_variable_name(in_file, 'standard_name', in_variable_standard_name,
                                                             verbose=False)
    in_variable = in_data.variables[in_variable_name]
    in_variable_data = in_variable[...]
    try:
        in_variable_data_units = in_variable_data.units
    except AttributeError:
        in_variable_data_units = ''
    if (in_variable_standard_name == 'upward_sea_water_velocity') and \
            ((in_variable_data_units == 'm/s') or (in_variable_data_units == 'm s-1')):
        in_variable_data *= 1000
    if np.ma.is_masked(in_variable_data):
        in_variable_data = np.ma.array(np.where(in_variable_data.data == in_variable_data.fill_value, out_fill_value,
                                                in_variable_data),
                                       mask=in_variable_data.mask, fill_value=out_fill_value,
                                       dtype=in_variable_data.dtype)
    else:
        in_variable_data = np.ma.array(in_variable_data, mask=np.zeros(shape=in_variable_data.shape, dtype=bool),
                                       fill_value=out_fill_value, dtype=in_variable_data.dtype)

    if in_variable_data.mask.all():
        time.sleep(sleep_time)
        print(' Warning: all data is missing in this variable.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    if time_rounding and (len(duplicated_indices) > 0):
        out_variable_data = in_variable_data[delete_mask, ...]
    else:
        out_variable_data = in_variable_data

    out_month_series = out_time_series.month
    # noinspection PyUnresolvedReferences
    monthly_mask = np.zeros(shape=(12, + out_time_series.shape[0]), dtype=bool)
    for month in range(12):
        monthly_mask[month, ...] = out_month_series == month + 1
    range_check_authorized = range_check_enabled
    if in_variable_standard_name == 'sea_water_practical_salinity':
        range_minimum = 5.
        range_maximum = 41.
    elif in_variable_standard_name == 'sea_water_temperature':
        range_minimum = 4.
        range_maximum = 32.
    else:
        if verbose:
            time.sleep(sleep_time)
            print(' Warning: range check minimum and maximum values not tuned yet for this variable.'
                  ' Skipping range check.', file=sys.stderr)
            time.sleep(sleep_time)
        range_minimum = None
        range_maximum = None
        range_check_authorized = False

    out_stat = dict()
    out_stat['data_total'] = str(out_variable_data.size)
    out_stat['filled_data'] = str(np.sum(out_variable_data.mask))

    detrended_variable_data = dict()
    monthly_mean_climatology_data = dict()
    monthly_std_climatology_data = dict()
    trend_information_data = dict()
    filtered_data = dict()
    filtered_density_data = dict()
    good_data = dict()
    rejected_data_percentage_profile = dict()
    good_qc_data = dict()
    if update_mode and (routine_qc_iterations >= 1):
        try:
            climatology_data = netCDF4.Dataset(climatology_file, mode='r')
            if verbose:
                print(' Opening historical monthly climatology average and standard deviation dataset.')
            monthly_mean_climatology_average =\
                climatology_data.variables[in_variable_standard_name + '_mm_clim']
            monthly_mean_climatology_standard_deviation =\
                climatology_data.variables[in_variable_standard_name + '_ms_clim']
            trend_history_data = \
                climatology_data.variables[in_variable_standard_name + '_trend']
            filtered_density = \
                climatology_data.variables[in_variable_standard_name + '_filtered_density']
            climatology_data.close()
        except FileNotFoundError:
            time.sleep(sleep_time)
            print(' Warning: platform climatology file not found..',
                  file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return

    try:
        in_data_institution = in_data.institution
    except AttributeError:
        in_data_institution = None
    ispra_temperature_stuck_test = False
    if 'ISPRA' in in_data_institution and in_variable_standard_name == 'sea_water_temperature':
        ispra_temperature_stuck_test = True

    for iteration in range(routine_qc_iterations + 1):
        rejected_data_mask = np.zeros(out_variable_data.shape, dtype=bool)
        if iteration == 0:
            good_data[iteration - 1] = np.ma.copy(out_variable_data)
            good_qc_data[iteration - 1] = np.array(1 + out_variable_data.mask * 3)

        if update_mode and (iteration >= 1):
            monthly_mean_climatology_data[iteration - 1] = monthly_mean_climatology_average[...]
            monthly_std_climatology_data[iteration - 1] = monthly_mean_climatology_standard_deviation[...]
        else:
            monthly_mean_climatology_data[iteration - 1] = np.ma.masked_all(shape=(12, in_depth_data.shape[-1]),
                                                                            dtype=np.float32)
            monthly_std_climatology_data[iteration - 1] = np.ma.masked_all(shape=(12, in_depth_data.shape[-1]),
                                                                           dtype=np.float32)
            for month in range(12):
                if not np.invert(monthly_mask[month, ...]).all():
                    monthly_mean_climatology_data[iteration - 1][month, :] = \
                        np.ma.mean(good_data[iteration - 1][monthly_mask[month, ...]], axis=0)
                    monthly_std_climatology_data[iteration - 1][month, :] = \
                        np.ma.std(good_data[iteration - 1][monthly_mask[month, ...]], axis=0)

        monthly_mean_series_iteration = monthly_mean_climatology_data[iteration - 1][out_month_series - 1, :]
        # monthly_std_series_iteration = monthly_std_climatology_data[iteration - 1][out_month_series - 1, :]

        filtered_data[iteration - 1] = \
            (good_data[iteration - 1] - monthly_mean_series_iteration)  # / monthly_std_series_iteration

        # Removing linear trend from data
        detrended_variable_data[iteration - 1] = np.ma.copy(filtered_data[iteration - 1])
        trend_information_data[iteration - 1] = np.ma.masked_all(shape=(in_depth_data.shape[-1], 2),
                                                                 dtype=np.float32)
        for depth in range(in_depth_data.shape[-1]):
            if update_mode and (iteration >= 1):
                trend_information_data[iteration - 1][depth].mask = False
                trend_information_data[iteration - 1][depth] = trend_history_data[depth]
                trend_line = trend_information_data[iteration - 1][depth][0] * out_time_data + \
                    trend_information_data[iteration - 1][depth][1]
            else:
                data_selection = np.invert(detrended_variable_data[iteration - 1].mask[..., depth])
                if np.all(np.invert(data_selection)):
                    trend_information_data[iteration - 1][depth].mask = False
                    trend_information_data[iteration - 1][depth] = np.array([0, 0])
                    continue
                not_filled_time_data = out_time_data[data_selection]
                not_filled_time_series = detrended_variable_data[iteration - 1][data_selection, depth]
                try:
                    regression_model = \
                        LinearRegression().fit(not_filled_time_data[:, np.newaxis], not_filled_time_series)
                    regression_model_score = \
                        regression_model.score(not_filled_time_data[:, np.newaxis], not_filled_time_series)
                    if regression_model_score < 0:
                        print(' Warning: regression score too low.', file=sys.stderr)
                        trend_information_data[iteration - 1][depth].mask = False
                        trend_information_data[iteration - 1][depth] = np.array([0, 0])
                        continue
                    trend_information_data[iteration - 1][depth].mask = False
                    trend_information_data[iteration - 1][depth] = \
                        np.array([regression_model.coef_[0] * 86400 * 365,
                                  np.mean(filtered_data[iteration - 1][data_selection, depth])])
                    trend_line = regression_model.predict(out_time_data[..., np.newaxis])
                except ValueError:
                    print(' Warning: detrending failed. Using direct data instead.', file=sys.stderr)
                    trend_information_data[iteration - 1][depth].mask = False
                    trend_information_data[iteration - 1][depth] = np.array([0, 0])
                    continue
            detrended_time_series = detrended_variable_data[iteration - 1][..., depth] - trend_line
            detrended_variable_data[iteration - 1][..., depth] = detrended_time_series

            filtered_data[iteration - 1][..., depth] = \
                np.ma.copy(detrended_variable_data[iteration - 1][..., depth])

        distribution_variable = np.ma.copy(filtered_data[iteration - 1])
        density_samples = np.arange(-10, 10, delta_x)
        if update_mode and (iteration >= 1):
            filtered_density_data[iteration - 1] = filtered_density
        else:
            filtered_density_data[iteration - 1] = \
                np.empty(shape=(density_samples.shape[0], in_depth_data.shape[-1]),
                         dtype=distribution_variable.dtype)
            # start_run_time_iteration = calendar.timegm(time.gmtime())
            for depth in range(in_depth_data.shape[-1]):
                kernel_density = \
                    KernelDensity(kernel='gaussian', bandwidth=0.2).fit(distribution_variable[..., depth, np.newaxis])
                # noinspection PyUnresolvedReferences
                filtered_density_data[iteration - 1][..., depth] = \
                    np.exp(kernel_density.score_samples(density_samples[..., np.newaxis]))
        # Define data distribution AS density (approximation)
        filtered_distribution = filtered_density_data[iteration - 1]
        interpolated_data = interpolate.interp1d(density_samples, filtered_distribution,
                                                 kind='linear', axis=0,
                                                 bounds_error=False, fill_value=out_fill_value)
        interpolated_distribution = interpolated_data(distribution_variable.data)[..., 0]
        interpolated_distribution = \
            np.ma.masked_where(interpolated_distribution == out_fill_value, interpolated_distribution)
        if iteration == 0:
            if range_check_authorized:
                # 1) Range check
                if verbose:
                    print(' Compute range check.')
                range_test_data = np.ma.filled(good_data[iteration - 1], range_minimum)
                out_of_range_mask = range_test_data < range_minimum
                out_of_range_mask = \
                    np.logical_or(out_of_range_mask, range_test_data > range_maximum)
                out_of_range_data_number = len(np.where(out_of_range_mask)[0])
                out_stat['range_check_rejection'] = str(out_of_range_data_number)
                out_of_range_data_percentage = \
                    np.round(out_of_range_data_number / good_qc_data[iteration - 1].size * 100, decimals=2)
                if verbose:
                    print(' Rejected data by range check for all depth levels: '
                          + str(out_of_range_data_number) + ' (' + str(out_of_range_data_percentage) + '%)')
                rejected_data_mask = np.logical_or(rejected_data_mask, out_of_range_mask)
                range_checked_data = np.ma.array(good_data[iteration - 1], mask=rejected_data_mask)
            else:
                range_checked_data = np.ma.copy(good_data[iteration - 1])
            # 2) Spike test
            if spike_test_enabled:
                if verbose:
                    print(' Compute spike test.')
                spike_test_data = np.ma.copy(range_checked_data)
                spiked_test_maximum = np.ma.masked_all(spike_test_data.shape)
                spiked_test_minimum = np.ma.masked_all(spike_test_data.shape)
                spiked_test_sum = np.ma.array(np.zeros(spike_test_data.shape),
                                              mask=np.ones(spike_test_data.shape, dtype=bool))
                spiked_test_count = np.ma.array(np.zeros(spike_test_data.shape),
                                                mask=np.ones(spike_test_data.shape, dtype=bool))
                for point in range(1, spiked_neighborhood_test_points + 1):
                    spiked_test_maximum[: -point, ...] = np.ma.max([spiked_test_maximum[: -point, ...],
                                                                    spike_test_data[point:, ...]], axis=0)
                    spiked_test_maximum[point:, ...] = np.ma.max([spiked_test_maximum[point:, ...],
                                                                  spike_test_data[: -point]], axis=0)
                    spiked_test_minimum[: -point, ...] = np.ma.min([spiked_test_minimum[: -point, ...],
                                                                    spike_test_data[point:, ...]], axis=0)
                    spiked_test_minimum[point:, ...] = np.ma.min([spiked_test_minimum[point:, ...],
                                                                  spike_test_data[: -point]], axis=0)
                    spiked_test_sum.mask[: -point, ...][np.invert(spike_test_data[point:, ...].mask)] = False
                    spiked_test_sum.data[: -point, ...] += spike_test_data[point:, ...]
                    spiked_test_sum.mask[point:, ...][np.invert(spike_test_data[: -point].mask)] = False
                    spiked_test_sum.data[point:, ...] += spike_test_data[: -point, ...]
                    spiked_test_count.mask[: -point, ...][np.invert(spike_test_data[point:, ...].mask)] = False
                    spiked_test_count.data[: -point, ...][np.invert(spike_test_data[point:, ...].mask)] += 1
                    spiked_test_count.mask[point:, ...][np.invert(spike_test_data[: -point].mask)] = False
                    spiked_test_count.data[point:, ...][np.invert(spike_test_data[: -point].mask)] += 1

                disabled_spike_test_mask = np.logical_or(spike_test_data.mask,
                                                         np.ma.filled(spiked_test_count, fill_value=0)
                                                         < 2 * spiked_neighborhood_test_points)
                spiked_test_range = np.ma.masked_where(disabled_spike_test_mask,
                                                       spiked_test_maximum - spiked_test_minimum)
                spiked_test_average = np.ma.masked_where(disabled_spike_test_mask,
                                                         spiked_test_sum / spiked_test_count)
                spiked_data_mask = \
                    np.abs(spike_test_data - spiked_test_average) > spiked_range_multiplier * spiked_test_range
                spiked_data_mask = np.ma.filled(spiked_data_mask, fill_value=False)
                spiked_data_number = len(np.where(spiked_data_mask)[0])
                out_stat['spike_test_rejection'] = str(spiked_data_number)
                spiked_data_percentage = \
                    np.round(spiked_data_number / good_qc_data[iteration - 1].size * 100, decimals=2)
                if verbose:
                    print(' Rejected data by spike test for all depth levels: '
                          + str(spiked_data_number) + ' (' + str(spiked_data_percentage) + '%)')
                rejected_data_mask = np.logical_or(rejected_data_mask, spiked_data_mask)
                spike_checked_data = np.ma.array(good_data[iteration - 1], mask=rejected_data_mask)
            else:
                spike_checked_data = np.ma.copy(good_data[iteration - 1])
            # 3) Stuck values test
            if stuck_value_test_enabled:
                if verbose:
                    print(' Compute stuck value test.')
                stuck_values_mask = np.zeros(shape=rejected_data_mask.shape, dtype=bool)
                for depth in range(in_depth_data.shape[-1]):
                    [unique_values, unique_counts] = \
                        np.unique(spike_checked_data[..., depth].data, return_counts=True)
                    try:
                        out_fill_value_index = np.where(unique_values == out_fill_value)[0][0]
                        unique_values = np.delete(unique_values, out_fill_value_index)
                        unique_counts = np.delete(unique_counts, out_fill_value_index)
                    except (IndexError, TypeError):
                        pass
                    # stuck value test
                    if stuck_value_test_enabled:
                        neighborhood_max_counts = np.zeros(unique_counts.shape)
                        for point in range(1, stuck_neighborhood_test_points + 1):
                            neighborhood_max_counts[: -point] = np.maximum(neighborhood_max_counts[: -point],
                                                                           unique_counts[point:])
                            neighborhood_max_counts[point:] = np.maximum(neighborhood_max_counts[point:],
                                                                         unique_counts[: -point])
                        stuck_values_bool = \
                            np.logical_and(unique_counts > stuck_minimum_count,
                                           unique_counts > stuck_values_multiplier * neighborhood_max_counts)
                    stuck_values = unique_values[stuck_values_bool]
                    # stuck_counts = unique_counts[stuck_values_bool]
                    if ispra_temperature_stuck_test and (20. in unique_values) and (20. not in stuck_values):
                        stuck_values = np.append(stuck_values, 20.)
                        # stuck_counts = np.append(stuck_counts, unique_counts[unique_values == 20.])
                    if verbose and len(stuck_values) > 0:
                        print(' Detected possible stuck values at level ' + str(depth) + ': ' + str(stuck_values))
                    stuck_values_mask[..., depth] = \
                        np.any(spike_checked_data.data[..., depth, np.newaxis] == stuck_values, axis=-1)

                stuck_values_number = len(np.where(stuck_values_mask)[0])
                out_stat['stuck_value_rejection'] = str(stuck_values_number)
                stuck_values_percentage = \
                    np.round(stuck_values_number / good_qc_data[iteration - 1].size * 100, decimals=2)
                if verbose:
                    print(' Rejected data by stuck value test for all depth levels: '
                          + str(stuck_values_number) + ' (' + str(stuck_values_percentage) + '%)')
                rejected_data_mask = np.logical_or(rejected_data_mask, stuck_values_mask)
                stuck_value_checked_data = np.ma.array(good_data[iteration - 1], mask=rejected_data_mask)
            else:
                stuck_value_checked_data = np.ma.copy(good_data[iteration - 1])
        else:
            # Statistic QC low probable test
            if verbose:
                if not update_mode:
                    print(' Compute out of statistic quality check ' + str(iteration) + '.')
                else:
                    print(' Compute out of climatological statistic quality check ' + str(iteration) + '.')
            statistic_data_mask = \
                interpolated_distribution < statistic_probability_threshold * delta_x
            high_data_mask = np.ones(shape=good_data[iteration - 1].shape, dtype=bool)
            for depth in range(in_depth_data.shape[-1]):
                for month in range(12):
                    valid_data_depth_month = \
                        np.where(np.invert(good_data[iteration - 1].mask[monthly_mask[month, ...], depth]))[0]
                    if len(valid_data_depth_month) * sampling_time_seconds / 86400. < valid_data_minimum_days:
                        high_data_mask[monthly_mask[month, ...], depth] = False

            statistic_data_mask = np.logical_and(statistic_data_mask, high_data_mask)
            statistic_data_number = len(np.where(statistic_data_mask)[0])
            statistic_data_mask = np.ma.filled(statistic_data_mask, fill_value=False)
            out_stat['statistic_rejection_' + str(iteration)] = str(statistic_data_number)
            statistic_data_percentage = \
                np.round(statistic_data_number / good_qc_data[iteration - 1].size * 100, decimals=2)
            if verbose:
                print(' Rejected data at iteration ' + str(iteration) + ' for all depth levels: '
                      + str(statistic_data_number) + ' (' + str(statistic_data_percentage) + '%)')
            rejected_data_mask = np.logical_or(rejected_data_mask, statistic_data_mask)

        good_data[iteration] = np.ma.copy(good_data[iteration - 1])
        good_data[iteration][rejected_data_mask] = out_fill_value
        good_data[iteration] = np.ma.masked_where(good_data[iteration] == out_fill_value, good_data[iteration])

        good_qc_data[iteration] = np.copy(good_qc_data[iteration - 1])
        if good_qc_data[iteration][rejected_data_mask].size > 0:
            good_qc_data[iteration][rejected_data_mask] = 4

        good_qc_data[iteration] = \
            np.ma.masked_where(good_qc_data[iteration] == out_fill_value, good_qc_data[iteration])

        rejected_data_percentage_profile[iteration] = np.empty(shape=in_depth_data.shape[-1])
        for depth in range(in_depth_data.shape[-1]):
            rejected_data_depth_number = len(np.where(rejected_data_mask[..., depth])[0])
            rejected_data_percentage_profile[iteration][depth] = \
                np.round(rejected_data_depth_number / good_qc_data[iteration - 1][:, depth].size * 100, decimals=2)

        if iteration == routine_qc_iterations:
            if update_mode and (iteration >= 1):
                monthly_mean_climatology_data[iteration] = monthly_mean_climatology_average[...]
                monthly_std_climatology_data[iteration] = monthly_mean_climatology_standard_deviation[...]
            else:
                monthly_mean_climatology_data[iteration] = np.ma.masked_all(shape=(12, in_depth_data.shape[-1]),
                                                                            dtype=np.float32)
                monthly_std_climatology_data[iteration] = np.ma.masked_all(shape=(12, in_depth_data.shape[-1]),
                                                                           dtype=np.float32)
                for month in range(12):
                    if not np.invert(monthly_mask[month, ...]).all():
                        monthly_mean_climatology_data[iteration][month, :] = \
                            np.ma.mean(good_data[iteration][monthly_mask[month, ...]], axis=0)
                        monthly_std_climatology_data[iteration][month, :] = \
                            np.ma.std(good_data[iteration][monthly_mask[month, ...]], axis=0)

            monthly_mean_series_iteration = monthly_mean_climatology_data[iteration][out_month_series - 1, :]
            # monthly_std_series_iteration = monthly_std_climatology_data[iteration][out_month_series - 1, :]

            filtered_data[iteration] = \
                (good_data[iteration] - monthly_mean_series_iteration)  # / monthly_std_series_iteration

            # Removing linear trend from data
            detrended_variable_data[iteration] = np.ma.copy(filtered_data[iteration])
            trend_information_data[iteration] = np.ma.masked_all(shape=(in_depth_data.shape[-1], 2), dtype=np.float32)
            for depth in range(in_depth_data.shape[-1]):
                if update_mode and (iteration >= 1):
                    trend_information_data[iteration][depth].mask = False
                    trend_information_data[iteration][depth] = trend_history_data[depth]
                    trend_line = trend_information_data[iteration][depth][0] * out_time_data + \
                        trend_information_data[iteration][depth][1]
                else:
                    data_selection = np.invert(detrended_variable_data[iteration].mask[..., depth])
                    if np.all(np.invert(data_selection)):
                        trend_information_data[iteration][depth].mask = False
                        trend_information_data[iteration][depth] = np.array([0, 0])
                        continue
                    not_filled_time_data = out_time_data[data_selection]
                    not_filled_time_series = detrended_variable_data[iteration][data_selection, depth]
                    try:
                        regression_model = \
                            LinearRegression().fit(not_filled_time_data[:, np.newaxis], not_filled_time_series)
                        regression_model_score = \
                            regression_model.score(not_filled_time_data[:, np.newaxis], not_filled_time_series)
                        if regression_model_score < 0:
                            print(' Warning: regression score too low.', file=sys.stderr)
                            trend_information_data[iteration][depth].mask = False
                            trend_information_data[iteration][depth] = np.array([0, 0])
                            continue
                        trend_information_data[iteration][depth].mask = False
                        trend_information_data[iteration][depth] = \
                            np.array([regression_model.coef_[0] * 86400 * 365,
                                      np.mean(filtered_data[iteration][data_selection, depth])])
                        trend_line = regression_model.predict(out_time_data[..., np.newaxis])
                    except ValueError:
                        print(' Warning: detrending failed. Using direct data instead.', file=sys.stderr)
                        trend_information_data[iteration][depth].mask = False
                        trend_information_data[iteration][depth] = np.array([0, 0])
                        continue

                detrended_time_series = detrended_variable_data[iteration][..., depth] - trend_line
                detrended_variable_data[iteration][..., depth] = detrended_time_series
                filtered_data[iteration][..., depth] = \
                    np.ma.copy(detrended_variable_data[iteration][..., depth])

            distribution_variable = np.ma.copy(filtered_data[iteration])
            density_samples = np.arange(-10, 10, delta_x)
            if update_mode and (iteration >= 1):
                filtered_density_data[iteration] = filtered_density
            else:
                filtered_density_data[iteration] = \
                    np.empty(shape=(density_samples.shape[0], in_depth_data.shape[-1]),
                             dtype=distribution_variable.dtype)
                # start_run_time_iteration = calendar.timegm(time.gmtime())
                for depth in range(in_depth_data.shape[-1]):
                    kernel_density = \
                        KernelDensity(kernel='gaussian', bandwidth=0.2).fit(
                            distribution_variable[..., depth, np.newaxis])
                    # noinspection PyUnresolvedReferences
                    filtered_density_data[iteration - 1][..., depth] = \
                        np.exp(kernel_density.score_samples(density_samples[..., np.newaxis]))

        # time_diff_iteration = time.gmtime(calendar.timegm(time.gmtime()) - start_run_time_iteration)
        # if verbose:
        #     print(' -------------------------')
        #     print(' Routine QC iteration ' + str(iteration) + ' completed. ETA is '
        #           + time.strftime('%H:%M:%S', time_diff_iteration))
        #     print(' -------------------------')

    # Plotting part (OPTIONAL)
    if plotting:
        for depth in range(in_depth_data.shape[-1]):
            # plt.plot(out_time_series, out_variable_data[:, depth, ...], label='Original data')
            plt.plot(out_time_series, range_checked_data[:, depth, ...], label='Range check')
            plt.plot(out_time_series, spike_checked_data[:, depth, ...], label='Spike test')
            plt.plot(out_time_series, stuck_value_checked_data[:, depth, ...], label='Stuck value test')
            plt.legend()
            plt.title(os.path.basename(in_file) + ' gross check at level ' + str(in_depth_data[depth]) + 'm')
            manager = plt.get_current_fig_manager()
            manager.window.wm_geometry("+1920+0")
            manager.resize(1680, 1050)
            plt.show()

            # plt.plot(out_time_series, out_variable_data[:, depth, ...], label='Original data')
            for iteration in range(routine_qc_iterations + 1):
                if iteration == 0:
                    plt.plot(out_time_series, good_data[iteration][:, depth, ...], label='Gross check')
                else:
                    plt.plot(out_time_series, good_data[iteration][:, depth, ...],
                             label='Statistic QC ' + str(iteration))

                if iteration == routine_qc_iterations:
                    plt.legend()
                    plt.title(os.path.basename(in_file) + ' data analysis at level ' + str(in_depth_data[depth]) + 'm')
                    manager = plt.get_current_fig_manager()
                    manager.window.wm_geometry("+1920+0")
                    manager.resize(1680, 1050)
                    plt.show()

            for iteration in range(routine_qc_iterations + 1):
                if iteration == 0:
                    plt.plot(out_time_series, filtered_data[iteration][:, depth, ...], label='Gross check')
                else:
                    plt.plot(out_time_series, filtered_data[iteration][:, depth, ...],
                             label='Statistic QC ' + str(iteration))

                if iteration == routine_qc_iterations:
                    plt.legend()
                    plt.title(os.path.basename(in_file) + ' filtered data at level '
                              + str(in_depth_data[depth]) + 'm')
                    manager = plt.get_current_fig_manager()
                    manager.window.wm_geometry('+1920+0')
                    manager.resize(1680, 1050)
                    plt.show()

            for iteration in range(routine_qc_iterations + 1):
                if iteration == 0:
                    plt.plot(density_samples, filtered_density_data[iteration][:, depth, ...],
                             label='Gross check')
                else:
                    plt.plot(density_samples, filtered_density_data[iteration][:, depth, ...],
                             label='Statistic QC ' + str(iteration))

                if iteration == routine_qc_iterations:
                    plt.legend()
                    plt.title(os.path.basename(in_file) + ' filtered data density at level '
                              + str(in_depth_data[depth]) + 'm')
                    manager = plt.get_current_fig_manager()
                    manager.window.wm_geometry("+1920+0")
                    manager.resize(1680, 1050)
                    plt.show()

    if verbose:
        print(' Creating output dataset.')
    # Create output dataset
    out_data = netCDF4.Dataset(out_file, mode='w', format='NETCDF4')

    if verbose:
        print(' Creating dimensions.')
    # Create new dimensions
    out_data.createDimension('depth', in_depth_data.shape[-1])
    out_data.createDimension('time', None)
    out_data.createDimension('month', 12)
    out_data.createDimension('iteration', routine_qc_iterations + 1)
    out_data.createDimension('samples', len(density_samples))
    out_data.createDimension('axis_nbounds', 2)

    # Creating dimension variables
    out_dimension_variables = ['lon', 'lat', 'depth', 'time']
    for dimension_variable_name in out_dimension_variables:
        in_dimension_variable = in_data.variables[dimension_variable_name]
        in_dimension_variable_data = in_dimension_variable[...]
        if verbose:
            print(' Attaching dimension variable ' + dimension_variable_name)
        if 'time' in in_dimension_variable.dimensions:
            out_dimension_variable = out_data.createVariable(dimension_variable_name, in_dimension_variable.datatype,
                                                             dimensions=in_dimension_variable.dimensions,
                                                             zlib=True, complevel=1)
            if dimension_variable_name == 'time':
                out_dimension_variable_data = out_time_series.values.astype('datetime64[s]')
            elif time_rounding and (in_dimension_variable_data.shape[0] > len(out_time_series)):
                delete_mask = np.ones(in_dimension_variable_data.shape[0], dtype=bool)
                delete_mask[duplicated_indices] = False
                out_dimension_variable_data = in_dimension_variable_data[delete_mask, ...]
            else:
                out_dimension_variable_data = in_dimension_variable_data
        else:
            out_dimension_variable = out_data.createVariable(dimension_variable_name, in_dimension_variable.datatype,
                                                             dimensions=in_dimension_variable.dimensions)
            out_dimension_variable_data = in_dimension_variable_data
        out_dimension_variable[...] = out_dimension_variable_data
        variable_attributes = [attribute for attribute in in_dimension_variable.ncattrs()
                               if attribute not in '_FillValue']
        out_dimension_variable.setncatts({attribute: in_dimension_variable.getncattr(attribute)
                                          for attribute in variable_attributes})
        if dimension_variable_name == 'depth':
            if out_dimension_variable[...].shape[-1] > 1:
                out_dimension_variable.valid_min = np.float32(np.min(out_dimension_variable[...]))
                out_dimension_variable.valid_max = np.float32(np.max(out_dimension_variable[...]))

    if verbose:
        print(' Attaching dimension variable month')
    out_month = out_data.createVariable('month', 'c', dimensions=('month',))
    out_month[...] = np.array(['January', 'February', 'March', 'April', 'May', 'June', 'July',
                               'August', 'September', 'October', 'November', 'December'], dtype=object)
    out_month.long_name = 'Solar Months'
    out_month.standard_name = 'month'
    out_month.units = 'dimensionless'

    if verbose:
        print(' Attaching dimension variable iteration')
    out_iteration = out_data.createVariable('iteration', 'i2', dimensions=('iteration',))
    out_iteration[...] = np.arange(routine_qc_iterations + 1)
    out_iteration.long_name = 'QC Routine Iterations'
    out_iteration.standard_name = 'iterations'
    out_iteration.units = 'dimensionless'
    out_iteration.values = ' iteration = 0: gross check QC, iteration j >= 1: statistical QC phase j'

    # if routine_qc_iterations >= 1:
    #     out_standard_deviation_limits = out_data.createVariable('standard_deviation_maximum_profile',
    #                                                             datatype=np.float32, dimensions=in_depth.dimensions,
    #                                                             fill_value=out_fill_value, zlib=True, complevel=1)
    #     out_standard_deviation_limits[...] = standard_deviation_threshold
    #     out_standard_deviation_limits.long_name = 'Standard Deviation Absolute Maximum Profile'
    #     out_standard_deviation_limits.standard_name = 'standard_deviation_maximum_profile'
    #     out_standard_deviation_limits.units = 'sigma'

    if verbose:
        print(' Attaching dimension variable samples')
    out_samples = out_data.createVariable('samples', np.float32, dimensions=('samples',))
    out_samples[...] = density_samples
    out_samples.long_name = 'Samples For Density Function'
    out_samples.standard_name = 'density_function_samples'
    out_samples.units = in_variable.units

    # Create new variables and set attributes
    if verbose:
        print(' Creating original values variable.')
    original_variable = out_data.createVariable(in_variable_standard_name, datatype=np.float32,
                                                dimensions=('time', 'depth'),
                                                fill_value=out_fill_value, zlib=True, complevel=1)
    original_variable[...] = out_variable_data
    original_variable.missing_value = np.float32(out_fill_value)
    original_variable.long_name = in_variable.long_name
    original_variable.standard_name = in_variable.standard_name
    original_variable.units = in_variable.units
    if in_variable_standard_name == 'upward_sea_water_velocity':
        original_variable.units = 'mm s-1'
    original_variable.valid_min = np.float32(np.ma.min(out_variable_data))
    original_variable.valid_max = np.float32(np.ma.max(out_variable_data))

    if verbose:
        print(' Creating iterative rejected data percentage profile.')
    out_rejected_data_percentage_profile_variable = out_data.createVariable('rejected_data_percentage_profile',
                                                                            datatype=np.float32,
                                                                            dimensions=('depth', 'iteration'))
    for iteration in range(routine_qc_iterations + 1):
        out_rejected_data_percentage_profile_variable[..., iteration] = rejected_data_percentage_profile[iteration]
    out_rejected_data_percentage_profile_variable.long_name = 'Rejected Data Percentage Vertical Profile'
    out_rejected_data_percentage_profile_variable.standard_name = 'rejected_data_percentage_profile'
    out_rejected_data_percentage_profile_variable.units = 'percentile'
    out_rejected_data_percentage_profile_variable.valid_min =\
        np.float32(np.ma.min(out_rejected_data_percentage_profile_variable[...]))
    out_rejected_data_percentage_profile_variable.valid_max =\
        np.float32(np.ma.max(out_rejected_data_percentage_profile_variable[...]))
    if verbose:
        print(' Creating iterative QC values variable.')
    out_variable_qc = out_data.createVariable(in_variable_standard_name + '_qc', datatype=np.float32,
                                              dimensions=('time', 'depth', 'iteration'),
                                              zlib=True, complevel=1)
    for iteration in range(routine_qc_iterations + 1):
        out_variable_qc[..., iteration] = good_qc_data[iteration]
    out_variable_qc.long_name = 'quality flag'
    out_variable_qc.flag_values = '1, 4'
    out_variable_qc.flag_meanings = '1 = good_data, 4 = bad_data'
    out_variable_qc.valid_min = np.float32(np.min(out_variable_qc[...]))
    out_variable_qc.valid_max = np.float32(np.max(out_variable_qc[...]))

    if verbose:
        print(' Creating iterative monthly mean climatology values variable.')
    monthly_mean_climatology_variable = out_data.createVariable(in_variable_standard_name + '_mm_clim',
                                                                datatype=np.float32,
                                                                dimensions=('month', 'depth', 'iteration'),
                                                                fill_value=out_fill_value,
                                                                zlib=True, complevel=1)
    for iteration in range(routine_qc_iterations + 1):
        monthly_mean_climatology_variable[..., iteration] = monthly_mean_climatology_data[iteration]
    monthly_mean_climatology_variable.missing_value = np.float32(out_fill_value)
    monthly_mean_climatology_variable.long_name = 'Monthly Climatology Iterative Average'
    monthly_mean_climatology_variable.standard_name = 'monthly_mean_climatology'
    monthly_mean_climatology_variable.units = in_variable.units
    monthly_mean_climatology_variable.valid_min = \
        np.float32(np.ma.min(monthly_mean_climatology_variable[...]))
    monthly_mean_climatology_variable.valid_max =\
        np.float32(np.ma.max(monthly_mean_climatology_variable[...]))

    if verbose:
        print(' Creating iterative monthly standard deviation climatology variable.')
    monthly_std_climatology_variable = \
        out_data.createVariable(in_variable_standard_name + '_ms_clim',  datatype=np.float32,
                                dimensions=('month', 'depth', 'iteration'), fill_value=out_fill_value)
    for iteration in range(routine_qc_iterations + 1):
        monthly_std_climatology_variable[..., iteration] = monthly_std_climatology_data[iteration]
    monthly_std_climatology_variable.missing_value = np.float32(out_fill_value)
    monthly_std_climatology_variable.long_name = 'Monthly Climatology Iterative Standard Deviation'
    monthly_std_climatology_variable.standard_name = 'monthly_std_climatology'
    monthly_std_climatology_variable.units = in_variable.units
    monthly_std_climatology_variable.valid_min = \
        np.float32(np.ma.min(monthly_std_climatology_variable[...]))
    monthly_std_climatology_variable.valid_max = \
        np.float32(np.ma.max(monthly_std_climatology_variable[...]))

    if verbose:
        print(' Creating iterative trend information data profile.')
    out_trend_information_variable = out_data.createVariable(in_variable_standard_name + '_trend',
                                                             datatype=np.float32,
                                                             dimensions=('depth', 'axis_nbounds', 'iteration'))
    for iteration in range(routine_qc_iterations + 1):
        out_trend_information_variable[..., iteration] = trend_information_data[iteration]
    out_trend_information_variable.long_name = 'Trend Information Data Vertical Profile'
    out_trend_information_variable.standard_name = 'trend_information_data'
    out_trend_information_variable.units = in_variable.units
    out_trend_information_variable.valid_min = \
        np.float32(np.ma.min(out_trend_information_variable[...]))
    out_rejected_data_percentage_profile_variable.valid_max = \
        np.float32(np.ma.max(out_trend_information_variable[...]))

    if verbose:
        print(' Creating iterative filtered data variable.')
    filtered_variable = \
        out_data.createVariable(in_variable_standard_name + '_filtered', datatype=np.float32,
                                dimensions=('time', 'depth', 'iteration'),
                                fill_value=out_fill_value, zlib=True, complevel=1)
    for iteration in range(routine_qc_iterations + 1):
        filtered_variable[..., iteration] = filtered_data[iteration]
    filtered_variable.missing_value = np.float32(out_fill_value)
    filtered_variable.long_name = 'Iterative Filtered Data'
    filtered_variable.standard_name = 'filtered_data'
    filtered_variable.units = in_variable.units
    filtered_variable.valid_min = \
        np.float32(np.ma.min(filtered_variable[...]))
    filtered_variable.valid_max = \
        np.float32(np.ma.max(filtered_variable[...]))

    if verbose:
        print(' reating iterative filtered data density variable.')
    filtered_density_variable = \
        out_data.createVariable(in_variable_standard_name + '_filtered_density', datatype=np.float32,
                                dimensions=('samples', 'depth', 'iteration'),
                                fill_value=out_fill_value, zlib=True, complevel=1)
    for iteration in range(routine_qc_iterations + 1):
        filtered_density_variable[..., iteration] = filtered_density_data[iteration]
    filtered_density_variable.missing_value = np.float32(out_fill_value)
    filtered_density_variable.long_name = 'Iterative Filtered Data Density'
    filtered_density_variable.standard_name = 'filtered_data_density'
    filtered_density_variable.units = 'dimensionless'
    filtered_density_variable.valid_min = \
        np.float32(np.ma.min(filtered_density_variable[...]))
    filtered_density_variable.valid_max = \
        np.float32(np.ma.max(filtered_density_variable[...]))

    if verbose:
        print(' Setting global attributes.')
    # Set global attributes
    global_attributes = [element for element in in_data.ncattrs() if not element.startswith('history')]
    out_data.setncatts({attr: in_data.getncattr(attr) for attr in global_attributes})

    out_data.history = in_data.history
    if range_minimum is not None:
        out_data.history = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + \
            ' : Signal spikes QC created (values below ' + str(range_minimum) + \
            ' and above ' + str(range_maximum) + 'flagged as bad\n' + out_data.history
    if routine_qc_iterations == 1:
        out_data.history = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + \
            ' : Statistic QC variables created\n' + out_data.history
    elif routine_qc_iterations > 1:
        out_data.history = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + \
            ' : ' + str(routine_qc_iterations) + ' phases statistic QC variables created\n' + out_data.history

    out_data.variable_type = 'Original observed'
    if verbose:
        print(' Closing output dataset.')
        print(' -------------------------')
    out_data.close()

    if not update_mode:
        if verbose:
            print(' Creating climatology dataset.')
        # Create output dataset
        out_climatology_data = netCDF4.Dataset(climatology_file, mode='w', format='NETCDF4')

        if verbose:
            print(' Creating dimensions.')
        # Create new dimensions
        out_climatology_data.createDimension('depth', in_depth_data.shape[-1])
        out_climatology_data.createDimension('time', None)
        out_climatology_data.createDimension('axis_nbounds', 2)
        out_climatology_data.createDimension('samples', len(density_samples))

        climatology_time_data = np.empty(shape=0)
        climatology_bounds_data = np.empty(shape=(0, 2))
        time_start_year = str(int(np.min(out_time_series.year)))
        time_average_year = str(int(np.round(np.mean(out_time_series.year))))
        time_end_year = str(int(np.max(out_time_series.year)))
        for month in range(1, 13):
            if month < 10:
                time_string = '-0' + str(month) + '-01'
            else:
                time_string = '-' + str(month) + '-01'

            climatology_time = calendar.timegm(time.strptime(time_average_year + time_string, '%Y-%m-%d'))
            climatology_time_data = np.append(climatology_time_data, climatology_time)
            climatology_bounds = \
                np.array([calendar.timegm(time.strptime(time_start_year + time_string, '%Y-%m-%d')),
                          calendar.timegm(time.strptime(time_end_year + time_string, '%Y-%m-%d'))])[np.newaxis, ...]
            climatology_bounds_data = np.append(climatology_bounds_data, climatology_bounds, axis=0)

        # Creating dimension variables
        out_dimension_variables = ['lon', 'lat', 'depth', 'time']
        for dimension_variable_name in out_dimension_variables:
            in_dimension_variable = in_data.variables[dimension_variable_name]
            in_dimension_variable_data = in_dimension_variable[...]
            if verbose:
                print(' Attaching dimension variable ' + dimension_variable_name)
            out_dimension_variable = out_climatology_data.createVariable(dimension_variable_name,
                                                                         in_dimension_variable.datatype,
                                                                         dimensions=in_dimension_variable.dimensions)
            if dimension_variable_name == 'time':

                out_dimension_variable_data = climatology_time_data
                variable_attributes = \
                    [attribute for attribute in in_dimension_variable.ncattrs() if attribute not in 'time_bounds']
                out_dimension_variable.climatology = 'climatology_bounds'
            else:
                out_dimension_variable_data = in_dimension_variable_data
                variable_attributes = in_dimension_variable.ncattrs()
            out_dimension_variable[...] = out_dimension_variable_data
            variable_attributes = [attribute for attribute in variable_attributes
                                   if attribute not in '_FillValue']
            out_dimension_variable.setncatts({attribute: in_dimension_variable.getncattr(attribute)
                                              for attribute in variable_attributes})
            if dimension_variable_name == 'depth':
                if out_dimension_variable[...].shape[-1] > 1:
                    out_dimension_variable.valid_min = np.float32(np.min(out_dimension_variable[...]))
                    out_dimension_variable.valid_max = np.float32(np.max(out_dimension_variable[...]))
        # Creating climatology_bounds variable
        if verbose:
            print(' Attaching dimension variable climatology_bounds')
        out_climatology_bounds = out_climatology_data.createVariable('climatology_bounds', in_time.datatype,
                                                                     dimensions=('time', 'axis_nbounds'))
        out_climatology_bounds[...] = climatology_bounds_data
        out_climatology_bounds.units = in_time_reference

        if verbose:
            print(' Attaching dimension variable samples')
        out_samples = out_climatology_data.createVariable('samples', np.float32, dimensions=('samples',))
        out_samples[...] = density_samples
        out_samples.long_name = 'Samples For Density Function'
        out_samples.standard_name = 'density_function_samples'
        out_samples.units = in_variable.units

        # Create new variables and set attributes
        if verbose:
            print(' Creating monthly mean climatology values variable.')
        monthly_mean_climatology_variable = out_climatology_data.createVariable(in_variable_standard_name + '_mm_clim',
                                                                                datatype=np.float32,
                                                                                dimensions=('time', 'depth'),
                                                                                fill_value=out_fill_value,
                                                                                zlib=True, complevel=1)
        monthly_mean_climatology_variable[...] = monthly_mean_climatology_data[routine_qc_iterations]
        monthly_mean_climatology_variable.missing_value = np.float32(out_fill_value)
        monthly_mean_climatology_variable.long_name = 'Monthly Climatology Average'
        monthly_mean_climatology_variable.standard_name = 'monthly_mean_climatology'
        monthly_mean_climatology_variable.units = in_variable.units
        monthly_mean_climatology_variable.valid_min = \
            np.float32(np.ma.min(monthly_mean_climatology_variable[...]))
        monthly_mean_climatology_variable.valid_max = \
            np.float32(np.ma.max(monthly_mean_climatology_variable[...]))

        if verbose:
            print(' Creating monthly standard deviation climatology variable.')
        monthly_std_climatology_variable = \
            out_climatology_data.createVariable(in_variable_standard_name + '_ms_clim', datatype=np.float32,
                                                dimensions=('time', 'depth'), fill_value=out_fill_value)
        monthly_std_climatology_variable[...] = monthly_std_climatology_data[routine_qc_iterations]
        monthly_std_climatology_variable.missing_value = np.float32(out_fill_value)
        monthly_std_climatology_variable.long_name = 'Monthly Climatology Standard Deviation'
        monthly_std_climatology_variable.standard_name = 'monthly_std_climatology'
        monthly_std_climatology_variable.units = in_variable.units
        monthly_std_climatology_variable.valid_min = \
            np.float32(np.ma.min(monthly_std_climatology_variable[...]))
        monthly_std_climatology_variable.valid_max = \
            np.float32(np.ma.max(monthly_std_climatology_variable[...]))

        if verbose:
            print(' Creating trend information data profile.')
        trend_information_variable = out_climatology_data.createVariable(in_variable_standard_name + '_trend',
                                                                         datatype=np.float32,
                                                                         dimensions=('depth', 'axis_nbounds'))
        trend_information_variable[...] = trend_information_data[routine_qc_iterations]
        trend_information_variable.long_name = 'Trend Information Data Vertical Profile'
        trend_information_variable.standard_name = 'trend_information_data'
        trend_information_variable.units = in_variable.units
        trend_information_variable.valid_min = \
            np.float32(np.ma.min(trend_information_variable[...]))
        trend_information_variable.valid_max = \
            np.float32(np.ma.max(trend_information_variable[...]))

        if verbose:
            print(' Creating filtered data density variable.')
        filtered_density_variable = \
            out_climatology_data.createVariable(in_variable_standard_name + '_filtered_density', datatype=np.float32,
                                                dimensions=('samples', 'depth'),
                                                fill_value=out_fill_value, zlib=True, complevel=1)
        filtered_density_variable[...] = filtered_density_data[routine_qc_iterations]
        filtered_density_variable.missing_value = np.float32(out_fill_value)
        filtered_density_variable.long_name = 'Filtered Data Density'
        filtered_density_variable.standard_name = 'filtered_data_density'
        filtered_density_variable.units = 'dimensionless'
        filtered_density_variable.valid_min = \
            np.float32(np.ma.min(filtered_density_variable[...]))
        filtered_density_variable.valid_max = \
            np.float32(np.ma.max(filtered_density_variable[...]))

        if verbose:
            print(' Setting global attributes.')
        # Set global attributes
        global_attributes = [element for element in in_data.ncattrs() if not element.startswith('history')]
        out_climatology_data.setncatts({attr: in_data.getncattr(attr) for attr in global_attributes})

        out_climatology_data.history = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + \
            ' : Climatology variables created\n' + in_data.history

        out_climatology_data.variable_type = 'Climatologies'

        if verbose:
            print(' Closing datasets.')
            print(' -------------------------')
        out_climatology_data.close()

    if verbose:
        print(' Closing input dataset.')
        print(' -------------------------')
    # Close input dataset
    in_data.close()

    return out_stat


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_file = sys.argv[1]
        in_variable_standard_name = sys.argv[2]
        update_mode = string_to_bool(sys.argv[3])
        routine_qc_iterations = int(sys.argv[4])
        out_file = sys.argv[5]
        climatology_file = sys.argv[6]

    except (IndexError, ValueError):
        in_file = None
        in_variable_standard_name = None
        update_mode = None
        routine_qc_iterations = None
        out_file = None
        climatology_file = None

    try:
        verbose = string_to_bool(sys.argv[7])
    except (IndexError, ValueError):
        verbose = True

    time_series_post_processing(in_file, in_variable_standard_name, update_mode, routine_qc_iterations,
                                climatology_file, out_file, verbose)
