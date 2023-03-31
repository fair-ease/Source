# -*- coding: utf-8 -*-
import sys
import os
import time
import numpy as np
import seawater as sw
import netCDF4
from SOURCE import find_variable_name

# Global variables
sleep_time = 0.1  # seconds
out_fill_value = 1.e20


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


def ptmp_to_temp(ptmp_file=None, salt_file=None, temp_file=None, verbose=True):
    if __name__ == '__main__':
        return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to compute sea water temperature from potential temperature and salinity.')
        print(' -------------------------')
    if ptmp_file is None or salt_file is None or temp_file is None:
        time.sleep(sleep_time)
        print(' Error: 3 of 4 maximum arguments (1 optional) not provided.', file=sys.stderr)
        print(' 1) input potential temperature dataset in netCDF format;', file=sys.stderr)
        print(' 2) input salinity dataset in netCDF format;', file=sys.stderr)
        print(' 3) output temperature dataset in netCDF format;', file=sys.stderr)
        print(' 4) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    if verbose:
        print(' Input potential temperature dataset = ' + ptmp_file)
        print(' Input salinity dataset = ' + salt_file)
        print(' Output temperature dataset = ' + temp_file)
        print(' Verbosity switch = ' + str(verbose))
        print(' -------------------------')
        print(' Starting process...')
        print(' -------------------------')
        print(' Opening potential temperature dataset.')

    try:
        ptmp_variable_name = find_variable_name.find_variable_name(ptmp_file, 'standard_name',
                                                                   'sea_water_potential_temperature', verbose=False)
    except KeyError:
        try:
            ptmp_variable_name = find_variable_name.find_variable_name(ptmp_file, 'standard_name', 'temperature',
                                                                       verbose=False)
        except KeyError:
            try:
                ptmp_variable_name = find_variable_name.find_variable_name(ptmp_file, 'long_name', 'temperature',
                                                                           verbose=False)
            except KeyError:
                time.sleep(sleep_time)
                print(' Error. Potential temperature variable not found in input dataset. Exiting.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return

    ptmp_data = netCDF4.Dataset(ptmp_file, mode='r')

    ptmp_variable = ptmp_data.variables[ptmp_variable_name]

    try:
        ptmp_longitude = ptmp_data.variables['lon']
    except KeyError:
        try:
            ptmp_longitude = ptmp_data.variables['LONGITUDE']
        except KeyError:
            try:
                ptmp_longitude = ptmp_data.variables['nav_lon']
            except KeyError:
                time.sleep(sleep_time)
                print(' Warning. Potential temperature dataset longitude dimension variable not found. Exiting.',
                      file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
    ptmp_longitude_data = ptmp_longitude[...]

    try:
        ptmp_latitude = ptmp_data.variables['lat']
    except KeyError:
        try:
            ptmp_latitude = ptmp_data.variables['LATITUDE']
        except KeyError:
            try:
                ptmp_latitude = ptmp_data.variables['nav_lat']
            except KeyError:
                time.sleep(sleep_time)
                print(' Warning. Potential temperature dataset latitude dimension variable not found. Exiting.',
                      file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
    ptmp_latitude_data = ptmp_latitude[...]

    try:
        ptmp_depth = ptmp_data.variables['depth']
    except KeyError:
        try:
            ptmp_depth = ptmp_data.variables['DEPH']
        except KeyError:
            try:
                ptmp_depth = ptmp_data.variables['deptht']
            except KeyError:
                try:
                    ptmp_depth = ptmp_data.variables['depthu']
                except KeyError:
                    try:
                        ptmp_depth = ptmp_data.variables['depthv']
                    except KeyError:
                        try:
                            ptmp_depth = ptmp_data.variables['depthw']
                        except KeyError:
                            try:
                                ptmp_depth = ptmp_data.variables['nav_lev']
                            except KeyError:
                                time.sleep(sleep_time)
                                print(' Warning. Potential temperature dataset depth dimension variable not found.'
                                      ' (maybe is a surface dataset). Exiting.', file=sys.stderr)
                                time.sleep(sleep_time)
                                print(' -------------------------')
                                return
    ptmp_depth_data = ptmp_depth[...]

    try:
        ptmp_time = ptmp_data.variables['time']
    except KeyError:
        try:
            ptmp_time = ptmp_data.variables['TIME']
        except KeyError:
            try:
                ptmp_time = ptmp_data.variables['time_counter']
            except KeyError:
                time.sleep(sleep_time)
                print(' Warning. Potential temperature dataset time dimension variable not found. Exiting.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
    ptmp_time_data = ptmp_time[...]

    try:
        salt_variable_name = find_variable_name.find_variable_name(salt_file, 'standard_name', 'sea_water_salinity',
                                                                   verbose=False)
    except KeyError:
        try:
            salt_variable_name = find_variable_name.find_variable_name(salt_file, 'standard_name',
                                                                       'sea_water_practical_salinity', verbose=False)
        except KeyError:
            try:
                salt_variable_name = find_variable_name.find_variable_name(salt_file, 'standard_name', 'salinity',
                                                                           verbose=False)
            except KeyError:
                try:
                    salt_variable_name = find_variable_name.find_variable_name(salt_file, 'long_name', 'salinity',
                                                                               verbose=False)
                except KeyError:
                    time.sleep(sleep_time)
                    print(' Error. Salinity variable not found in input dataset. Exiting.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(' -------------------------')
                    return

    if verbose:
        print(' Opening salinity dataset.')
    salt_data = netCDF4.Dataset(salt_file, mode='r')

    salt_variable = salt_data.variables[salt_variable_name]

    try:
        salt_longitude = salt_data.variables['lon']
    except KeyError:
        try:
            salt_longitude = salt_data.variables['LONGITUDE']
        except KeyError:
            try:
                salt_longitude = salt_data.variables['nav_lon']
            except KeyError:
                time.sleep(sleep_time)
                print(' Warning. Salinity dataset longitude dimension variable not found. Exiting.',
                      file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
    salt_longitude_data = salt_longitude[...]

    try:
        salt_latitude = salt_data.variables['lat']
    except KeyError:
        try:
            salt_latitude = salt_data.variables['LATITUDE']
        except KeyError:
            try:
                salt_latitude = salt_data.variables['nav_lat']
            except KeyError:
                time.sleep(sleep_time)
                print(' Warning. Salinity dataset latitude dimension variable not found. Exiting.',
                      file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
    salt_latitude_data = salt_latitude[...]

    try:
        salt_depth = salt_data.variables['depth']
    except KeyError:
        try:
            salt_depth = salt_data.variables['DEPH']
        except KeyError:
            try:
                salt_depth = salt_data.variables['deptht']
            except KeyError:
                try:
                    salt_depth = salt_data.variables['depthu']
                except KeyError:
                    try:
                        salt_depth = salt_data.variables['depthv']
                    except KeyError:
                        try:
                            salt_depth = salt_data.variables['depthw']
                        except KeyError:
                            try:
                                salt_depth = salt_data.variables['nav_lev']
                            except KeyError:
                                time.sleep(sleep_time)
                                print(' Warning. Salinity dataset depth dimension variable not found.'
                                      ' (maybe is a surface dataset). Exiting.', file=sys.stderr)
                                time.sleep(sleep_time)
                                print(' -------------------------')
                                return
    salt_depth_data = salt_depth[...]

    try:
        salt_time = salt_data.variables['time']
    except KeyError:
        try:
            salt_time = salt_data.variables['TIME']
        except KeyError:
            try:
                salt_time = salt_data.variables['time_counter']
            except KeyError:
                time.sleep(sleep_time)
                print(' Warning. Salinity dataset time dimension variable not found. Exiting.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
    salt_time_data = salt_time[...]

    if not np.allclose(ptmp_longitude_data, salt_longitude_data):
        time.sleep(sleep_time)
        print(' Error. potential temperature and salinity longitude dimension variables are not equal.',
              file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    if not np.allclose(ptmp_latitude_data, salt_latitude_data):
        time.sleep(sleep_time)
        print(' Error. potential temperature and salinity latitude dimension variables are not equal.',
              file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    if not np.allclose(ptmp_depth_data, salt_depth_data):
        time.sleep(sleep_time)
        print(' Error. potential temperature and salinity depth dimension variables are not equal.',
              file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    temp_time_data = np.intersect1d(ptmp_time_data, salt_time_data)
    if temp_time_data.shape[0] == 0:
        time.sleep(sleep_time)
        print(' Warning: potential temperature and salinity variable have empty time dimension intersection.',
              file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    ptmp_index_time_data = \
        [index for index in range(ptmp_time_data.shape[0]) if ptmp_time_data[index] in temp_time_data]
    salt_index_time_data = \
        [index for index in range(salt_time_data.shape[0]) if salt_time_data[index] in temp_time_data]

    ptmp_variable_data = ptmp_variable[ptmp_index_time_data, ...]
    if not np.ma.is_masked(ptmp_variable_data):
        ptmp_variable_data = np.ma.array(ptmp_variable_data,
                                         mask=np.zeros(shape=ptmp_variable_data.shape, dtype=bool),
                                         fill_value=out_fill_value, dtype=ptmp_variable_data.dtype)

    salt_variable_data = salt_variable[salt_index_time_data, ...]
    if not np.ma.is_masked(salt_variable_data):
        salt_variable_data = np.ma.array(salt_variable_data,
                                         mask=np.zeros(shape=salt_variable_data.shape, dtype=bool),
                                         fill_value=out_fill_value, dtype=salt_variable_data.dtype)
    if verbose:
        print(' Computing water pressure.')
    if (not ptmp_latitude_data.shape) and (not ptmp_longitude_data.shape):
        depth_3d_data = ptmp_depth_data
    elif ptmp_depth_data.ndim == 1:
        depth_3d_data = np.repeat(ptmp_depth_data, ptmp_latitude_data.shape[0] * ptmp_longitude_data.shape[-1], axis=-1)
        depth_3d_data =\
            np.reshape(depth_3d_data,
                       (ptmp_depth_data.shape[0], ptmp_latitude_data.shape[0], ptmp_longitude_data.shape[-1]))
    elif ptmp_depth_data.ndim == 3:
        depth_3d_data = ptmp_depth_data
    if ptmp_latitude_data.ndim == 1:
        latitude_2d_data = np.repeat(ptmp_latitude_data, ptmp_longitude_data.shape[0], axis=-1)
        latitude_2d_data = np.reshape(latitude_2d_data, (ptmp_latitude_data.shape[0], ptmp_longitude_data.shape[0]))
    else:
        latitude_2d_data = ptmp_latitude_data
    pressure_data = sw.pres(depth_3d_data, latitude_2d_data)
    pressure_4d_data = np.repeat(pressure_data, temp_time_data.shape[0], axis=-1)
    # Place record dimension from first to last coordinate in pressure
    pressure_4d_data = np.reshape(pressure_4d_data, pressure_data.shape + (temp_time_data.shape[0],))
    # Place record dimension from first to last coordinate in ptmp and salt
    ptmp_variable_data = np.transpose(ptmp_variable_data, list(range(1, len(ptmp_variable_data.shape))) + [0])
    salt_variable_data = np.transpose(salt_variable_data, list(range(1, len(salt_variable_data.shape))) + [0])

    if verbose:
        print(' Computing sea water temperature variable.')
    temp_variable_data = sw.temp(salt_variable_data, ptmp_variable_data, pressure_4d_data)
    # Place record dimension from last to first coordinate in temp
    temp_variable_data = np.transpose(temp_variable_data, [-1] + list(range(len(temp_variable_data.shape) - 1)))
    temp_variable_data = np.ma.masked_where(np.abs(temp_variable_data) > out_fill_value / 1000, temp_variable_data)
    # Close salt dataset
    salt_data.close()

    if verbose:
        print(' Creating output dataset.')
    # Create output dataset
    temp_data = netCDF4.Dataset(temp_file, mode='w', format='NETCDF4')

    # Copy dimensions in output dataset
    if verbose:
        print(' Creating dimensions.')

    for dimension_name in ptmp_data.dimensions.keys():
        temp_data.createDimension(dimension_name, ptmp_data.dimensions[dimension_name].size
                                  if not ptmp_data.dimensions[dimension_name].isunlimited() else None)

    if verbose:
        print(' Creating fixed variables.')
    # Copy fixed variables in output dataset

    for variable_name in ptmp_data.variables.keys():
        if variable_name == ptmp_variable_name:
            continue
        ptmp_fixed_variable = ptmp_data.variables[variable_name]
        if variable_name == 'time':
            temp_fixed_variable = temp_data.createVariable(variable_name, ptmp_fixed_variable.datatype,
                                                           dimensions=ptmp_fixed_variable.dimensions,
                                                           zlib=True, complevel=1)
            temp_fixed_variable[...] = temp_time_data
        elif variable_name == 'time_bounds':
            temp_fixed_variable = temp_data.createVariable(variable_name, ptmp_fixed_variable.datatype,
                                                           dimensions=ptmp_fixed_variable.dimensions,
                                                           zlib=True, complevel=1)
            temp_fixed_variable[...] = ptmp_fixed_variable[ptmp_index_time_data, ...]
        elif 'time' in ptmp_fixed_variable.dimensions:
            temp_fixed_variable = temp_data.createVariable(variable_name, ptmp_fixed_variable.datatype,
                                                           dimensions=ptmp_fixed_variable.dimensions,
                                                           fill_value=out_fill_value, zlib=True, complevel=1)
            temp_fixed_variable[...] = ptmp_fixed_variable[ptmp_index_time_data, ...]
        elif ptmp_fixed_variable.ndim >= 2:
            temp_fixed_variable = temp_data.createVariable(variable_name, ptmp_fixed_variable.datatype,
                                                           dimensions=ptmp_fixed_variable.dimensions,
                                                           fill_value=out_fill_value, zlib=True, complevel=1)
            temp_fixed_variable[...] = ptmp_fixed_variable[...]
        else:
            temp_fixed_variable = temp_data.createVariable(variable_name, ptmp_fixed_variable.datatype,
                                                           dimensions=ptmp_fixed_variable.dimensions)
            temp_fixed_variable[...] = ptmp_fixed_variable[...]
        temp_fixed_variable[...] = ptmp_fixed_variable[...]
        variable_attributes =\
            [attribute for attribute in ptmp_fixed_variable.ncattrs() if attribute not in '_FillValue']
        temp_fixed_variable.setncatts({attribute: ptmp_fixed_variable.getncattr(attribute)
                                      for attribute in variable_attributes})

    if verbose:
        print(' Creating sea water temperature variable.')
    temp_variable = temp_data.createVariable('sea_water_temperature', ptmp_variable.datatype,
                                             dimensions=ptmp_variable.dimensions,
                                             fill_value=out_fill_value, zlib=True, complevel=1)
    temp_variable[...] = temp_variable_data
    temp_variable_attributes = [attribute for attribute in ptmp_variable.ncattrs() if attribute not in '_FillValue']
    temp_variable.setncatts({attribute: ptmp_variable.getncattr(attribute) for attribute in temp_variable_attributes})
    temp_variable.standard_name = 'sea_water_temperature'
    temp_variable.long_name = 'Sea Water In Situ Temperature'
    temp_variable.valid_min = np.float32(np.min(temp_variable_data))
    temp_variable.valid_max = np.float32(np.max(temp_variable_data))

    if verbose:
        print(' Setting global attributes.')
    # Set global attributes
    global_attributes = [element for element in ptmp_data.ncattrs() if not element.startswith('history')]
    temp_data.setncatts({attribute: ptmp_data.getncattr(attribute) for attribute in global_attributes})
    temp_data.history = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + \
        ' : Created sea water temperature from potential temperature and salinity datasets\n'
    temp_data.variable_type = 'Derived temperature field'
    if verbose:
        print(' Closing output dataset.')
        print(' -------------------------')
    # Close datasets
    ptmp_data.close()
    temp_data.close()


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        ptmp_file = sys.argv[1]
        salt_file = sys.argv[2]
        temp_file = sys.argv[3]

    except (IndexError, ValueError):
        ptmp_file = None
        salt_file = None
        temp_file = None

    try:
        verbose = string_to_bool(sys.argv[4])
    except (IndexError, ValueError):
        verbose = True

    ptmp_to_temp(ptmp_file, salt_file, temp_file, verbose)
