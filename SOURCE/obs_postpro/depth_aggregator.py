# -*- coding: utf-8 -*-
import sys
import os
import time
import calendar
import numpy as np
import netCDF4

# Global variables
sleep_time = 0.1  # seconds
out_fill_value = 1.e20
depth_minimum_space = 0.5  # m
depth_average_threshold = 5 / 100  # %
threshold_tolerance = 20 / 100  # %


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def depth_aggregator(in_file=None, depth_array_str=None, out_file=None,
                     first_date_str=None, last_date_str=None, verbose=True):
    # if __name__ == '__main__':
    #     return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' -------------------------')
        print(' Script to aggregate instantaneous depth variable and average horizontal coordinates'
              ' in observation files.')
        print(' -------------------------')
    if in_file is None or depth_array_str is None or out_file is None:
        time.sleep(sleep_time)
        print(' Error: 3 of 6 maximum arguments (3 optionals) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) input depth array string to compute aggregation;', file=sys.stderr)
        print(' 3) output file;', file=sys.stderr)
        print(' 4) (optional) first cut date in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: first recorded data);', file=sys.stderr)
        print(' 5) (optional) last cut date in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              '  default: last recorded data).', file=sys.stderr)
        print(' 6) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
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

    if verbose:
        print(' Input file = ' + in_file)
        print(' Depth array string ' + depth_array_str)
        print(' Output file = ' + out_file)
        print(' Verbosity switch = ' + str(verbose))
        print(' -------------------------')

    if first_date_str is not None:
        first_date_seconds = calendar.timegm(first_date)
    if last_date_str is not None:
        last_date_seconds = calendar.timegm(last_date)

    in_data = netCDF4.Dataset(in_file, mode='r')

    # Loading record coordinate
    record_dimension = None
    for dimension in in_data.dimensions:
        if in_data.dimensions[dimension].isunlimited():
            record_dimension = dimension
            break
    if record_dimension is None:
        try:
            record_dimension = 'TIME'
            in_time = in_data.variables[record_dimension]
        except KeyError:
            try:
                record_dimension = 'time'
                in_time = in_data.variables[record_dimension]
            except KeyError:
                time.sleep(sleep_time)
                print(' Error. Record dimension variable not found in input file.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
    else:
        try:
            in_time = in_data.variables[record_dimension]
        except KeyError:
            time.sleep(sleep_time)
            print(' Error. Record dimension variable not found in input file.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return
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
        time.sleep(sleep_time)
        print(' Warning: no data in the selected period for this variable.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    out_time_indices = np.where(out_time_mask)[0]

    # Retrieve depth variable
    in_depth = in_data.variables['depth']
    in_depth_data = np.round(in_depth[...], decimals=5)
    if not np.ma.is_masked(in_depth_data):
        in_depth_data = np.ma.array(in_depth_data,
                                    mask=np.zeros(shape=in_depth_data.shape, dtype=bool),
                                    fill_value=out_fill_value, dtype=in_depth_data.dtype)
    in_depth_data = in_depth_data[out_time_indices, ...]
    if verbose:
        print(' Starting process...')

    out_depth_data = np.array(np.float32(depth_array_str.split(' ')))

    print(' Creating output dataset.')
    # Create output dataset
    out_data = netCDF4.Dataset(out_file, mode='w', format='NETCDF4')

    # Copy fixed dimensions in output dataset
    if verbose:
        print(' Creating fixed dimensions.')
    out_fixed_dimensions = [dimension for dimension in in_data.dimensions.keys() if dimension not in 'depth']

    for dimension_name in out_fixed_dimensions:
        out_data.createDimension(dimension_name, in_data.dimensions[dimension_name].size
                                 if not in_data.dimensions[dimension_name].isunlimited() else None)

    if verbose:
        print(' Creating time dimension variable and averaging horizontal coordinates.')
    # Copy fixed variables in output dataset
    out_fixed_variables = ['lon', 'lat', 'time']

    for variable_name in out_fixed_variables:
        in_fixed_variable = in_data.variables[variable_name]
        if variable_name == 'time':
            out_fixed_variable = out_data.createVariable(variable_name, in_fixed_variable.datatype,
                                                         dimensions=in_fixed_variable.dimensions,
                                                         zlib=True, complevel=1)
            out_fixed_variable[...] = in_fixed_variable[out_time_indices]
            variable_attributes = [attribute for attribute in in_fixed_variable.ncattrs() if
                                   attribute not in '_FillValue']
            out_fixed_variable.setncatts({attribute: in_fixed_variable.getncattr(attribute)
                                          for attribute in variable_attributes})
        else:
            out_fixed_variable = out_data.createVariable(variable_name, in_fixed_variable.datatype)
            out_fixed_variable[...] = np.around(np.mean(in_fixed_variable[out_time_indices, ...]), decimals=2)
            variable_attributes = [attribute for attribute in in_fixed_variable.ncattrs() if
                                   attribute not in '_FillValue']
            out_fixed_variable.setncatts({attribute: in_fixed_variable.getncattr(attribute)
                                          for attribute in variable_attributes})
            out_fixed_variable.cell_methods = 'time: mean'
            out_fixed_variable.long_name = 'Average device ' + in_fixed_variable.standard_name

    if verbose:
        print(' Creating new depth dimension.')
    # Create new depth dimension
    out_data.createDimension('depth', out_depth_data.shape[0])

    if verbose:
        print(' Creating depth variable.')
    # Create new depth variable
    out_depth = out_data.createVariable('depth', in_depth.datatype,
                                        dimensions=('depth',))
    out_depth[...] = out_depth_data
    out_depth.positive = 'down'
    out_depth.long_name = 'depth'
    out_depth.standard_name = 'depth'
    out_depth.units = 'm'
    out_depth.axis = 'Z'

    if out_depth_data.shape[0] > 1:
        out_depth.valid_min = np.float32(np.min(out_depth_data))
        out_depth.valid_max = np.float32(np.max(out_depth_data))

    out_variables = [variable for variable in in_data.variables.keys()
                     if variable not in ['lon', 'lat', 'depth', 'time']]

    for variable_name in out_variables:
        in_variable = in_data.variables[variable_name]
        in_variable_data = in_variable[...]
        if not np.ma.is_masked(in_variable_data):
            in_variable_data = np.ma.array(in_variable_data,
                                           mask=np.zeros(shape=in_variable_data.shape, dtype=bool),
                                           fill_value=out_fill_value, dtype=float)
        in_variable_data = in_variable_data[out_time_indices]
        if verbose:
            print(' Aggregating depth levels on variable ' + variable_name)
        work_variable_data = np.ma.copy(in_variable_data)
        out_variable_data = np.ma.empty(shape=(in_variable_data.shape[0], 0),
                                        fill_value=out_fill_value, dtype=in_variable.dtype)
        for depth in out_depth_data:
            indices = np.abs(in_depth_data - depth) <= \
                      np.max([np.abs(depth) * depth_average_threshold, depth_minimum_space]) * \
                      (1 + threshold_tolerance)
            temp_variable = np.ma.masked_all(shape=in_variable_data.shape, dtype=in_variable.datatype)
            temp_variable = np.ma.where(indices, work_variable_data, temp_variable)
            mean_variable = np.ma.mean(temp_variable, axis=1)
            out_variable_data = np.ma.append(out_variable_data, mean_variable[:, np.newaxis], axis=1)
            work_variable_data[indices] = out_fill_value
            work_variable_data.mask[indices] = True

        # Create output variable
        out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                               dimensions=in_variable.dimensions,
                                               fill_value=out_fill_value, zlib=True, complevel=1)
        out_variable[...] = out_variable_data
        out_variable_attributes = [attribute for attribute in in_variable.ncattrs() if attribute not in '_FillValue']
        out_variable.setncatts({attribute: in_variable.getncattr(attribute) for attribute in out_variable_attributes})
        out_variable.valid_min = np.float32(np.min(out_variable_data))
        out_variable.valid_max = np.float32(np.max(out_variable_data))

    if verbose:
        print(' Setting global attributes.')
    # Set global attributes
    out_data.setncatts({attribute: in_data.getncattr(attribute) for attribute in in_data.ncattrs()})

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
        depth_array_str = sys.argv[2]
        out_file = sys.argv[3]
    except (IndexError, ValueError):
        in_file = None
        depth_array_str = None
        out_file = None

    try:
        first_date_str = sys.argv[4]
    except (IndexError, ValueError):
        first_date_str = None

    try:
        last_date_str = sys.argv[5]
    except (IndexError, ValueError):
        last_date_str = None

    try:
        verbose = string_to_bool(sys.argv[6])
    except (IndexError, ValueError):
        verbose = True

    depth_aggregator(in_file, depth_array_str, out_file, first_date_str, last_date_str, verbose)
