# -*- coding: utf-8 -*-
import sys
import os
import time
import numpy as np
from scipy import interpolate
import netCDF4

# Global variables
sleep_time = 0.1  # seconds
out_fill_value = 1.e20


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def vertical_interpolation(in_file=None, depth_array_str=None, out_file=None, verbose=True):
    if __name__ == '__main__':
        return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to compute vertical linear interpolation in depth dimension.')
        print(' -------------------------')
    if in_file is None or depth_array_str is None or out_file is None:
        time.sleep(sleep_time)
        print(' Error: 3 of 4 maximum arguments (1 optional) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) input depth array string to compute interpolation;', file=sys.stderr)
        print(' 3) output file;', file=sys.stderr)
        print(' 4) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    if verbose:
        print(' Input file = ' + in_file)
        print(' Output depth = ' + depth_array_str + 'm')
        print(' Output file = ' + out_file)
        print(' Verbosity switch = ' + str(verbose))
        print(' -------------------------')
        print(' Starting process...')
        print(' -------------------------')

    in_data = netCDF4.Dataset(in_file, mode='r')

    try:
        in_depth_dimension = in_data.dimensions['depth'].size
        depth_dimension_name = 'depth'
    except KeyError:
        try:
            in_depth_dimension = in_data.dimensions['DEPTH'].size
            depth_dimension_name = 'DEPTH'
        except KeyError:
            try:
                in_depth_dimension = in_data.dimensions['deptht'].size
                depth_dimension_name = 'deptht'
            except KeyError:
                try:
                    in_depth_dimension = in_data.dimensions['depthu'].size
                    depth_dimension_name = 'depthu'
                except KeyError:
                    try:
                        in_depth_dimension = in_data.dimensions['depthv'].size
                        depth_dimension_name = 'depthv'
                    except KeyError:
                        try:
                            in_depth_dimension = in_data.dimensions['depthw'].size
                            depth_dimension_name = 'depthw'
                        except KeyError:
                            try:
                                in_depth_dimension = in_data.dimensions['z'].size
                                depth_dimension_name = 'z'
                            except KeyError:
                                time.sleep(sleep_time)
                                print(' Warning. Input dataset depth dimension not found'
                                      ' (maybe is a surface dataset). Exiting.', file=sys.stderr)
                                time.sleep(sleep_time)
                                print(' -------------------------')
                                return
    try:
        longitude_name = in_data.variables['lon'].name
    except KeyError:
        try:
            longitude_name = in_data.variables['LONGITUDE'].name
        except KeyError:
            try:
                longitude_name = in_data.variables['nav_lon'].name
            except KeyError:
                time.sleep(sleep_time)
                print(' Warning. Input dataset longitude dimension variable not found. Exiting.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return

    try:
        latitude_name = in_data.variables['lat'].name
    except KeyError:
        try:
            latitude_name = in_data.variables['LATITUDE'].name
        except KeyError:
            try:
                latitude_name = in_data.variables['nav_lat'].name
            except KeyError:
                time.sleep(sleep_time)
                print(' Warning. Input dataset latitude dimension variable not found. Exiting.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return

    try:
        depth_name = in_data.variables['depth'].name
    except KeyError:
        try:
            depth_name = in_data.variables['DEPH'].name
        except KeyError:
            try:
                depth_name = in_data.variables['deptht'].name
            except KeyError:
                try:
                    depth_name = in_data.variables['depthu'].name
                except KeyError:
                    try:
                        depth_name = in_data.variables['depthv'].name
                    except KeyError:
                        try:
                            depth_name = in_data.variables['depthw'].name
                        except KeyError:
                            try:
                                depth_name = in_data.variables['nav_lev'].name
                            except KeyError:
                                time.sleep(sleep_time)
                                print(' Warning. Input dataset depth dimension variable not found.'
                                      ' (maybe is a surface dataset). Exiting.', file=sys.stderr)
                                time.sleep(sleep_time)
                                print(' -------------------------')
                                return

    try:
        time_name = in_data.variables['time'].name
    except KeyError:
        try:
            time_name = in_data.variables['TIME'].name
        except KeyError:
            try:
                time_name = in_data.variables['time_counter'].name
            except KeyError:
                time.sleep(sleep_time)
                print(' Warning. Input dataset time dimension variable not found. Exiting.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return

    if verbose:
        print(' Depth indices: ' + str(in_depth_dimension))

    # Retrieve depth variable
    in_depth = in_data.variables[depth_name]
    in_depth_data = np.round(in_depth[...], decimals=5)
    if not np.ma.is_masked(in_depth_data):
        in_depth_data = np.ma.array(in_depth_data,
                                    mask=np.zeros(shape=in_depth_data.shape, dtype=bool),
                                    fill_value=out_fill_value, dtype=in_depth_data.dtype)

    if verbose:
        print(' Input depth: from ' + str(np.int64(in_depth_data[0])) +
              'm to ' + str(np.int64(in_depth_data[-1])) + 'm')

    last_true_depth_index = -1
    for test_variable_name in in_data.variables.keys():
        test_variable = in_data.variables[test_variable_name]
        test_variable_data = test_variable[...]
        if not np.ma.is_masked(test_variable_data):
            test_variable_data = np.ma.array(test_variable_data,
                                             mask=np.zeros(shape=test_variable_data.shape, dtype=bool),
                                             fill_value=out_fill_value, dtype=float)
        for depth_index in range(len(in_depth_data)):
            if len(test_variable.shape) > 1 and (time_name in test_variable.dimensions) and \
                    (depth_name in test_variable.dimensions):
                if np.any(np.invert(test_variable_data.mask[..., depth_index])) and \
                        (last_true_depth_index < depth_index):
                    last_true_depth_index = depth_index

    if verbose:
        print(' Last depth with data: ' + str(np.round(np.float32(in_depth_data[last_true_depth_index]), decimals=2))
              + 'm')

    if verbose:
        print(' -------------------------')
        print(' Starting process...')

    out_depth_data = np.array(np.float32(depth_array_str.split(' ')))

    copy_surface_indices = np.empty(shape=0, dtype=np.int64)
    copy_bottom_indices = np.empty(shape=0, dtype=np.int64)
    for depth_index in range(len(out_depth_data)):
        if (out_depth_data[depth_index] < in_depth_data[0]) and \
                (in_depth_data[0] < 2 and out_depth_data[depth_index] < 2):
            copy_surface_indices = np.append(copy_surface_indices, depth_index)
    for depth_index in reversed(range(len(out_depth_data))):
        if (out_depth_data[depth_index] > in_depth_data[last_true_depth_index]) and \
                ((in_depth_data[last_true_depth_index] < 700 and out_depth_data[depth_index] <
                  in_depth_data[last_true_depth_index] * 1.1) or
                    (out_depth_data[depth_index] < in_depth_data[last_true_depth_index] * 1.2)):
            copy_bottom_indices = np.append(copy_bottom_indices, depth_index)

    if verbose:
        print(' Creating output dataset.')
    # Create output dataset
    out_data = netCDF4.Dataset(out_file, mode='w', format='NETCDF4')

    # Copy fixed dimensions in output dataset
    if verbose:
        print(' Creating fixed dimensions.')
    out_fixed_dimensions = [dimension for dimension in in_data.dimensions.keys()
                            if dimension not in depth_dimension_name]

    for dimension_name in out_fixed_dimensions:
        out_data.createDimension(dimension_name, in_data.dimensions[dimension_name].size
                                 if not in_data.dimensions[dimension_name].isunlimited() else None)

    if verbose:
        print(' Creating fixed dimension variables.')
    # Copy fixed variables in output dataset
    out_fixed_variables = [longitude_name, latitude_name, time_name]

    for variable_name in out_fixed_variables:
        in_fixed_variable = in_data.variables[variable_name]
        if variable_name == 'time':
            out_fixed_variable = out_data.createVariable(variable_name, in_fixed_variable.datatype,
                                                         dimensions=in_fixed_variable.dimensions,
                                                         zlib=True, complevel=1)
        else:
            out_fixed_variable = out_data.createVariable(variable_name, in_fixed_variable.datatype,
                                                         dimensions=in_fixed_variable.dimensions)
        out_fixed_variable[...] = in_fixed_variable[...]
        variable_attributes = [attribute for attribute in in_fixed_variable.ncattrs() if attribute not in '_FillValue']
        out_fixed_variable.setncatts({attribute: in_fixed_variable.getncattr(attribute)
                                      for attribute in variable_attributes})

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
    out_depth.long_name = 'Interpolated depth'
    out_depth.standard_name = 'depth'
    out_depth.units = 'm'
    out_depth.axis = 'Z'

    if out_depth_data.shape[0] > 1:
        out_depth.valid_min = np.float32(np.min(out_depth_data))
        out_depth.valid_max = np.float32(np.max(out_depth_data))

    out_variables = [variable for variable in in_data.variables.keys()
                     if variable not in [longitude_name, latitude_name, depth_name, time_name]]

    for variable_name in out_variables:
        in_variable = in_data.variables[variable_name]
        in_variable_data = in_variable[...]
        if not np.ma.is_masked(in_variable_data):
            in_variable_data = np.ma.array(in_variable_data,
                                           mask=np.zeros(shape=in_variable_data.shape, dtype=bool),
                                           fill_value=out_fill_value, dtype=float)
        if depth_dimension_name in in_variable.dimensions:
            for dimension in range(in_variable.ndim):
                if in_variable.shape[dimension] == in_depth_data.shape[-1]:
                    depth_dimension_axis = dimension
                    break
            if in_depth_data.shape[-1] > 1:
                if verbose:
                    print(' Interpolating depth dimension on variable ' + variable_name)
                interpolated_data = interpolate.interp1d(in_depth_data, in_variable_data.data, kind='linear',
                                                         axis=depth_dimension_axis,
                                                         bounds_error=False, fill_value=out_fill_value)
                out_variable_data = interpolated_data(out_depth_data)
                out_variable_data =\
                    np.ma.masked_where(np.abs(out_variable_data) > out_fill_value / 1000, out_variable_data)
            elif in_depth_data.shape[-1] == 1:
                if verbose:
                    print(' Input dataset have only one depth level.')
                if len(out_depth_data) > 1:
                    print(' Warning. Trying to interpolate one depth value with more than one depth values.' +
                          ' Skipping...', file=sys.stderr)
                    return
            for depth_index in copy_surface_indices:
                if verbose:
                    print('Copying data at depth ' +
                          str(np.round(np.float32(in_depth_data[0]), decimals=2)) +
                          'm to output data at depth ' +
                          str(np.round(np.float32(out_depth_data[depth_index]), decimals=2)) + 'm.')
                out_variable_data[..., depth_index] = in_variable_data[..., 0]
            for depth_index in copy_bottom_indices:
                if verbose:
                    print('Copying data at depth ' +
                          str(np.round(np.float32(in_depth_data[last_true_depth_index]), decimals=2)) +
                          'm to output data at depth ' +
                          str(np.round(np.float32(out_depth_data[depth_index]), decimals=2)) + 'm.')
                out_variable_data[..., depth_index] = in_variable_data[..., last_true_depth_index]
        else:
            out_variable_data = in_variable_data

        # Create output variable
        if variable_name == 'time_bounds':
            out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                   dimensions=in_variable.dimensions,
                                                   zlib=True, complevel=1)
        elif time_name in in_variable.dimensions:
            out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                   dimensions=in_variable.dimensions,
                                                   fill_value=out_fill_value, zlib=True, complevel=1)
            out_variable.valid_min = np.float32(np.min(out_variable_data))
            out_variable.valid_max = np.float32(np.max(out_variable_data))
        elif out_variable_data.ndim >= 2:
            out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                   dimensions=in_variable.dimensions)
            out_variable.valid_min = np.float32(np.min(out_variable_data))
            out_variable.valid_max = np.float32(np.max(out_variable_data))
        else:
            out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                   dimensions=in_variable.dimensions)
        out_variable[...] = out_variable_data
        out_variable_attributes = [attribute for attribute in in_variable.ncattrs() if attribute not in '_FillValue']
        out_variable.setncatts({attribute: in_variable.getncattr(attribute) for attribute in out_variable_attributes})

    if verbose:
        print(' Setting global attributes.')
    # Set global attributes
    global_attributes = [element for element in in_data.ncattrs() if not element.startswith('history')]
    out_data.setncatts({attribute: in_data.getncattr(attribute) for attribute in global_attributes})
    out_data.institution = 'Istituto Nazionale di Geofisica e Vulcanologia - Bologna, Italy'
    out_data.editor = 'Paolo Oliveri'
    out_data.contact = 'paolo.oliveri@ingv.it'
    out_data.Conventions = 'CF-1.6'
    out_data.field_type = 'Vertical interpolated model data'
    out_data.history = \
        time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + \
        ' : Computed vertical interpolation\n' + in_data.history

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
        verbose = string_to_bool(sys.argv[4])
    except (IndexError, ValueError):
        verbose = True

    vertical_interpolation(in_file, depth_array_str, out_file, verbose)
