# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
import numpy as np
import netCDF4
import datetime
import dateutil
import time
import calendar
from SOURCE import find_variable_name

# Global variables
sleep_time = 0.1  # seconds
out_fill_value = 1.e20
time_step_tolerance = 10  # percent, to accept datasets input sampling slightly greater than selected output sampling
out_time_reference = '1970-01-01T00:00:00Z'
out_reference_data = abs(calendar.timegm(time.strptime(out_time_reference, '%Y-%m-%dT%H:%M:%SZ')))


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


def round_down(datetime_variable, resolution):
    if resolution == 'AS':
        return datetime.datetime(year=datetime_variable.year, month=1, day=1)
    elif resolution == 'MS':
        return datetime.datetime(year=datetime_variable.year,
                                 month=datetime_variable.month, day=1)
    elif resolution == 'D':
        return datetime.datetime(year=datetime_variable.year,
                                 month=datetime_variable.month,
                                 day=datetime_variable.day)
    elif resolution == 'H':
        return datetime.datetime(year=datetime_variable.year,
                                 month=datetime_variable.month,
                                 day=datetime_variable.day,
                                 hour=datetime_variable.hour)
    elif resolution == 'T':
        return datetime.datetime(year=datetime_variable.year,
                                 month=datetime_variable.month,
                                 day=datetime_variable.day,
                                 hour=datetime_variable.hour,
                                 minute=datetime_variable.minute)
    elif resolution == 'S':
        return datetime.datetime(year=datetime_variable.year,
                                 month=datetime_variable.month,
                                 day=datetime_variable.day,
                                 hour=datetime_variable.hour,
                                 minute=datetime_variable.minute,
                                 second=datetime_variable.second)


def round_up(datetime_variable, resolution):
    if resolution == 'AS':
        return round_down(datetime_variable, resolution) + dateutil.relativedelta.relativedelta(years=1)
    if resolution == 'MS':
        return round_down(datetime_variable, resolution) + dateutil.relativedelta.relativedelta(months=1)
    elif resolution == 'D':
        return round_down(datetime_variable, resolution) + datetime.timedelta(days=1)
    elif resolution == 'H':
        return round_down(datetime_variable, resolution) + datetime.timedelta(hours=1)
    elif resolution == 'T':
        return round_down(datetime_variable, resolution) + datetime.timedelta(minutes=1)
    elif resolution == 'S':
        return round_down(datetime_variable, resolution) + datetime.timedelta(seconds=1)


def time_weighted_average(timeseries_data, resolution):
    try:
        timeseries_data.index[-1]
    except IndexError:
        return np.nan
    data_end = pd.DatetimeIndex([round_up(timeseries_data.index[-1], resolution)])
    indices = np.where(np.logical_not(np.isnan(timeseries_data)))[0]
    if indices.shape[0] > 0:
        return np.average(timeseries_data.to_numpy()[indices],
                          weights=np.diff(timeseries_data.index.append(data_end).asi8)[indices])
    else:
        return np.nan


# Functional version
def time_averager(in_file=None, average_step_str=None, out_file=None, in_variable_standard_name=None,
                  half_time_step_shift=False, verbose=True):
    if __name__ == '__main__':
        return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to compute custom weighted mean in oversampled observation time series files.')
        print(' -------------------------')
    if in_file is None or average_step_str is None or out_file is None:
        time.sleep(sleep_time)
        print(' Error: 3 of 6 maximum arguments (3 optionals) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) output average step:', file=sys.stderr)
        print('    accepted values:', file=sys.stderr)
        print('        a) hh:mm:ss;', file=sys.stderr)
        print('        b) MM (months number, if 12 it will be yearly average).', file=sys.stderr)
        print(' 3) output file;', file=sys.stderr)
        print(' 4) (optional) input variable standard_name to average (default: process all 1D - 2D variables);',
              file=sys.stderr)
        print(' 5) (optional) half time step average shifting (True or False) (default: False);', file=sys.stderr)
        print(' 6) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    if verbose:
        print(' Input file = ' + in_file)
        if len(average_step_str) >= 3:
            print(' Time step = ' + average_step_str + ' HH:MM:SS')
        else:
            print(' Time step = ' + average_step_str + ' months')
        print(' Output file = ' + out_file)
        print(' Input variable standard_name = ' + str(in_variable_standard_name) +
              ' (if None all 1D or 2D variables will be processed)')
        print(' Half time step shift for series is ' + str(half_time_step_shift))
        print(' Verbosity switch = ' + str(verbose))
        print(' -------------------------')

        print(' Opening input dataset.')
    # Open input dataset
    in_data = netCDF4.Dataset(in_file, mode='r')

    if in_variable_standard_name is not None:
        try:
            in_variable_name = find_variable_name.find_variable_name(in_file, 'standard_name',
                                                                     in_variable_standard_name, verbose=False)
        except KeyError:
            time.sleep(sleep_time)
            print(' Error. Input variable is not present in input dataset.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return

        if ('time' not in in_data.variables[in_variable_name].dimensions) or\
                (len(in_data.variables[in_variable_name].shape) < 1):
            time.sleep(sleep_time)
            print(' Error. Selected variable is not 1D, 2D or does not contain a record dimension.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return

    if verbose:
        print(' Depth indices: ' + str(in_data.dimensions['depth'].size))

    # Retrieve time variable
    in_time = in_data.variables['time']
    time_reference = in_time.units
    if 'days' in time_reference:
        in_time_data = np.round(in_time[...] * 86400.)
    elif 'seconds' in time_reference:
        in_time_data = np.round(in_time[...])
    time_reference = time_reference[time_reference.find('since ') + len('since '):]
    time_step_array = in_time_data[1:] - in_time_data[: -1]
    (time_step_values, time_step_counts) = np.unique(time_step_array, return_counts=True)

    zero_index = np.argwhere(time_step_values == 0.0)
    time_step_values = np.delete(time_step_values, zero_index)
    time_step = time_step_values[np.argmax(time_step_counts)]

    if verbose:
        time_step_years = str(int(time.strftime("%Y", time.gmtime(time_step))) - 1970)
        time_step_months = str(int(time.strftime("%m", time.gmtime(time_step))) - 1)
        time_step_days = str(int(time.strftime("%d", time.gmtime(time_step))) - 1)
        time_step_hours = time.strftime("%H:%M:%S", time.gmtime(time_step))
        time_step_str =\
            time_step_years + ' years ' + time_step_months + ' months ' + time_step_days + ' days '\
            + time_step_hours + ' HH:MM:SS'

        print(' Most representative time step: ' + time_step_str)

    if len(average_step_str) >= 3:
        out_average_str = ''
        out_time_delta = pd.Timedelta(average_step_str)
        half_time_delta = out_time_delta / 2
        days = 0
        hours = int(average_step_str.split(':')[0])
        minutes = int(average_step_str.split(':')[1])
        while minutes >= 60:
            hours += 1
            minutes -= 60
        while hours >= 24:
            days += 1
            hours -= 24
        if days > 0:
            out_average_str += str(days) + ' days:'
        if hours > 0:
            out_average_str += str(hours) + ' hours:'
        if minutes > 0:
            out_average_str += str(minutes) + ' minutes:'
        if minutes > 0:
            round_resolution = 'T'
        elif hours > 0:
            round_resolution = 'H'
        elif days > 0:
            round_resolution = 'D'
        freq_str = str(days) + 'D' + str(hours) + 'H' + str(minutes) + 'T0S'
        out_time_step = days * 86400 + hours * 3600 + minutes * 60
        half_days = half_time_delta.days
        half_hours = 0
        half_minutes = 0
        half_seconds = half_time_delta.seconds
        while half_seconds >= 60:
            half_minutes += 1
            half_seconds -= 60
        while half_minutes >= 60:
            half_hours += 1
            half_minutes -= 60
        while half_hours >= 24:
            half_days += 1
            half_hours -= 24
    else:
        out_average_str = average_step_str + ' months'
        months = int(average_step_str)
        out_time_step = months * 30 * 86400
        if months == 12:
            round_resolution = 'AS'
            freq_str = '1AS'
        else:
            round_resolution = 'MS'
            freq_str = str(months) + 'MS'

    if time_step > (1 + time_step_tolerance / 100) * out_time_step:
        time.sleep(sleep_time)
        print(' Error. Dataset time step and added tolerance are greater than specified output time step. Exiting.',
              file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    applied_tolerance = False
    if (time_step_tolerance > 0) and (time_step > out_time_step):
        time.sleep(sleep_time)
        print(' Warning. Dataset time step is greater than specified output time step but under tolerance.',
              ' Rounding down and averaging...', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        applied_tolerance = True

    if verbose:
        print(' Starting process...')
        print(' -------------------------')

        print(' Computing new time points.')
    # Get time reference and compute difference from it to pandas standard reference (1970-01-01T00:00:00Z)
    time_shift = np.float64(np.timedelta64(np.datetime64('1970-01-01') - np.datetime64(time_reference), 's'))

    # Get input and output time stamps for the two time series
    in_time_stamps = pd.Series((in_time_data - time_shift) * 1.e9, dtype='datetime64[ns]')

    # Round to nearest sub multiple of time step and remove generated duplicates to take into account of tolerance
    if applied_tolerance:
        if time_step < 29:
            round_frequency = '10ms'
        elif time_step < 60:
            round_frequency = '100ms'
        elif time_step < 10 * 60:
            round_frequency = 's'
        elif time_step < 3600:
            round_frequency = '10s'
        elif time_step < 10 * 3600:
            round_frequency = 'min'
        elif time_step < 86400:
            round_frequency = '10min'
        elif time_step < 10 * 86400:
            round_frequency = 'H'
        elif time_step < 31 * 86400:
            round_frequency = '10H'
        else:
            round_frequency = 'D'
        in_time_stamps = in_time_stamps.dt.round(round_frequency)
        [in_time_stamps, unique_indices] = \
            np.unique(in_time_stamps, return_index=True)
        duplicated_indices = np.unique(list(set(np.arange(in_time_stamps.shape[0])) - set(unique_indices)))
        in_time_stamps = pd.Series(in_time_stamps)
    start = round_down(in_time_stamps[0], round_resolution)
    end = round_down(in_time_stamps[in_time_stamps.shape[0] - 1], round_resolution)
    if half_time_step_shift:
        start -= pd.Timedelta(seconds=np.floor_divide(out_time_step, 2))
        end -= pd.Timedelta(seconds=np.floor_divide(out_time_step, 2))
    out_left_time_bounds = pd.date_range(start, end, freq=freq_str)
    if (round_resolution != 'MS') and (round_resolution != 'AS'):
        out_time_stamps = out_left_time_bounds + half_time_delta
        out_right_time_bounds = out_time_stamps + half_time_delta
    elif round_resolution == 'MS':
        out_time_stamps = out_left_time_bounds + pd.Timedelta(days=14)
        out_right_time_bounds = pd.date_range(start, end + dateutil.relativedelta.relativedelta(months=1), freq='M')
    elif round_resolution == 'AS':
        out_time_stamps = out_left_time_bounds + pd.Timedelta(days=182)
        out_right_time_bounds = pd.date_range(start, end + dateutil.relativedelta.relativedelta(years=1), freq='A')

    if verbose:
        print(' Creating output dataset.')
    # Create output dataset
    out_data = netCDF4.Dataset(out_file, mode='w', format='NETCDF4')

    if verbose:
        print(' Creating dimensions.')
    # Create new dimensions
    for dimension_name in in_data.dimensions:
        out_data.createDimension(dimension_name, in_data.dimensions[dimension_name].size
                                 if not in_data.dimensions[dimension_name].isunlimited() else None)

    try:
        out_data.createDimension('axis_nbounds', 2)
    except RuntimeError:
        pass


    if verbose:
        print(' Creating dimension variables.')

    out_dimension_variables = ['lon', 'lat', 'depth', 'time']

    for dimension_variable_name in out_dimension_variables:
        in_dimension_variable = in_data.variables[dimension_variable_name]
        if verbose:
            print(' Attaching dimension variable ' + dimension_variable_name)
        if dimension_variable_name == 'time':
            out_dimension_variable = out_data.createVariable(dimension_variable_name, in_dimension_variable.datatype,
                                                             dimensions=(dimension_variable_name,),
                                                             zlib=True, complevel=1)
            out_dimension_variable[...] = np.array([(t - np.datetime64('1970-01-01')).total_seconds()
                                                    for t in out_time_stamps]) + out_reference_data
            out_dimension_variable.long_name = 'Time'
            out_dimension_variable.standard_name = 'time'
            out_dimension_variable.units = 'seconds since ' + out_time_reference
            out_dimension_variable.calendar = 'gregorian'
            out_dimension_variable.cell_methods = 'time: ' + out_average_str + ' mean'
            out_dimension_variable.axis = 'T'
            out_dimension_variable.bounds = 'time_bounds'
        elif 'time' not in in_dimension_variable.dimensions:
            out_dimension_variable = out_data.createVariable(dimension_variable_name, in_dimension_variable.datatype,
                                                             dimensions=in_dimension_variable.dimensions)
            out_dimension_variable[...] = in_dimension_variable[...]
            variable_attributes = [attribute for attribute in in_dimension_variable.ncattrs()
                                   if attribute not in '_FillValue']
            out_dimension_variable.setncatts({attribute: in_dimension_variable.getncattr(attribute)
                                              for attribute in variable_attributes})
        if dimension_variable_name == 'depth':
            try:
                if out_dimension_variable[...].shape[0] > 1:
                    out_dimension_variable.valid_min =\
                        np.float32(np.min(out_dimension_variable[...]))
                    out_dimension_variable.valid_max =\
                        np.float32(np.max(out_dimension_variable[...]))
            except UnboundLocalError:
                pass

    print(' Attaching dimension variable time_bounds')
    out_time_bounds = out_data.createVariable('time_bounds', in_time.datatype,
                                              dimensions=('time', 'axis_nbounds'), zlib=True, complevel=1)
    out_time_bounds[..., 0] = np.array([(t - np.datetime64('1970-01-01')).total_seconds()
                                        for t in out_left_time_bounds]) + out_reference_data
    out_time_bounds[..., 1] = np.array([(t - np.datetime64('1970-01-01')).total_seconds()
                                        for t in out_right_time_bounds]) + out_reference_data
    out_time_bounds.units = 'seconds since ' + out_time_reference

    print(' Averaging selected variables.')

    # Compute selected variable average
    if in_variable_standard_name is not None:
        out_variables = ['lon', 'lat', 'depth', in_variable_name]
    else:
        out_variables = [variable for variable in in_data.variables.keys() if variable not in 'time']
    out_variables = [variable for variable in out_variables if 'time' in in_data.variables[variable].dimensions]

    for variable_name in out_variables:
        in_variable = in_data.variables[variable_name]
        in_variable_data = in_variable[...]
        if not np.ma.is_masked(in_variable_data):
            in_variable_data = np.ma.array(in_variable_data, mask=np.zeros(shape=in_variable_data.shape, dtype=bool),
                                           fill_value=out_fill_value, dtype=in_variable_data.dtype)

        # Remove duplicates if there is tolerance application
        if applied_tolerance and in_variable_data.shape[0] > len(in_time_stamps):
            delete_mask = np.ones(in_variable_data.shape, dtype=bool)
            delete_mask[duplicated_indices, ...] = False
            in_variable_data = in_variable_data[delete_mask, ...]
        # Compute time weighted average
        out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                               dimensions=in_variable.dimensions,
                                               fill_value=out_fill_value, zlib=True, complevel=1)
        variable_attributes = [attribute for attribute in in_variable.ncattrs() if attribute not in '_FillValue']
        out_variable.setncatts({attribute: in_variable.getncattr(attribute) for attribute in variable_attributes})
        if verbose:
            print(' Computing time weighted average for variable ' + variable_name)
        if len(in_variable_data.shape) == 2:
            out_time_series = np.ma.empty(shape=(out_time_stamps.shape[0], in_variable_data.shape[-1]),
                                          fill_value=out_fill_value, dtype=in_variable.dtype)
            for depth in range(in_variable_data.shape[-1]):
                if verbose:
                    print(' Depth index: ' + str(depth))
                in_variable_depth_slice = in_variable_data[:, depth, ...]
                in_depth_time_series = pd.Series(data=in_variable_depth_slice, index=in_time_stamps)
                # Add the vacant indices in input time series and place NaN values in them
                work_depth_time_series = in_depth_time_series.reindex(in_depth_time_series.index.union(out_time_stamps))
                # Compute the time average
                out_depth_time_series = work_depth_time_series.resample(freq_str).apply(time_weighted_average,
                                                                                        resolution=round_resolution)
                out_depth_time_series_data = np.ma.array(out_depth_time_series.to_numpy().real,
                                                         mask=np.isnan(out_depth_time_series.to_numpy().real),
                                                         fill_value=out_fill_value, dtype=in_variable.dtype)
                out_time_series[:, depth] = out_depth_time_series_data
        elif len(in_variable_data.shape) == 1:
            in_time_series = pd.Series(data=in_variable_data, index=in_time_stamps)
            # Add the vacant indices in input time series and place NaN values in them
            work_time_series = in_time_series.reindex(in_time_series.index.union(out_time_stamps))
            # Compute the time average
            out_time_series = work_time_series.resample(freq_str).apply(time_weighted_average,
                                                                        resolution=round_resolution)
            out_time_series = np.ma.array(out_time_series.to_numpy().real,
                                          mask=np.isnan(out_time_series.to_numpy().real),
                                          fill_value=out_fill_value, dtype=in_variable.dtype)
        out_variable[...] = out_time_series
        out_variable.cell_methods = 'time: ' + out_average_str + ' mean'
        out_variable.valid_min = np.float32(np.ma.min(out_time_series))
        out_variable.valid_max = np.float32(np.ma.max(out_time_series))

    if verbose:
        print(' Setting global attributes.')
    # Set global attributes
    global_attributes = [element for element in in_data.ncattrs() if not element.startswith('history')]
    out_data.setncatts({attribute: in_data.getncattr(attribute) for attribute in global_attributes})

    if applied_tolerance:
        out_data.history = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + \
            ' : Computed ' + out_average_str + ' weighted average with tolerance of ' + \
            str(time_step_tolerance) + '% on dataset time step\n' + in_data.history
    else:
        out_data.history = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + \
            ' : Computed ' + out_average_str + ' weighted average\n' + in_data.history

    if verbose:
        print(' Closing datasets.')
        print(' -------------------------')
    # Close input and output datasets
    in_data.close()
    out_data.close()


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_file = sys.argv[1]
        average_step_str = sys.argv[2]
        out_file = sys.argv[3]

    except (IndexError, ValueError):
        in_file = None
        average_step_str = None
        out_file = None

    try:
        in_variable_standard_name = sys.argv[4]
    except (IndexError, ValueError):
        in_variable_standard_name = None

    try:
        half_time_step_shift = string_to_bool(sys.argv[5])
    except (IndexError, ValueError):
        half_time_step_shift = False

    try:
        verbose = string_to_bool(sys.argv[6])
    except (IndexError, ValueError):
        verbose = True

    time_averager(in_file, average_step_str, out_file, in_variable_standard_name, half_time_step_shift, verbose)
