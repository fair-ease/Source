# -*- coding: utf-8 -*-
import sys
import os
import numpy as np
import seawater as sw
import netCDF4
import time
import calendar
from SOURCE import find_variable_name

# Global variables
sleep_time = 0.1  # seconds
out_fill_value = 1.e20
out_time_reference = '1970-01-01T00:00:00Z'
out_reference_data = abs(calendar.timegm(time.strptime(out_time_reference, '%Y-%m-%dT%H:%M:%SZ')))


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def insitu_tac_timeseries_extractor(in_file=None, in_variable_standard_name=None, out_file=None,
                                    first_date_str=None, last_date_str=None, verbose=True):
    if __name__ == '__main__':
        return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to extract a field from CMEMS INSITU TAC insitu observations netCDF files.')
        print(' -------------------------')
    if in_file is None or in_variable_standard_name is None or out_file is None:
        time.sleep(sleep_time)
        print(' Error: 3 of 6 maximum arguments (3 optionals) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) input variable standard_name;', file=sys.stderr)
        print(' 3) output file;', file=sys.stderr)
        print(' 4) (optional) first cut date in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: first recorded data);', file=sys.stderr)
        print(' 5) (optional) last cut date in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              '  default: last recorded data).', file=sys.stderr)
        print(' 6) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    if verbose:
        print(' Input file = ' + in_file)
        print(' Input variable standard_name = ' + in_variable_standard_name)
        print(' Output file = ' + out_file)
        print(' First date to process = ' + str(first_date_str) +
              ' (if None it will be the first available date)')
        print(' Last date to process = ' + str(last_date_str) +
              ' (if None it will be the last available date)')
        print(' Verbosity switch = ' + str(verbose))

        print(' Starting process...')
        print(' -------------------------')

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
    if verbose:
        print(' Opening input dataset.')

    # Open input dataset
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

    try:
        in_time_qc = in_data.variables['TIME_QC']
        in_time_qc_data = in_time_qc[...]
        if np.ma.is_masked(in_time_qc_data):
            in_time_qc_data = in_time_qc_data.data
        time_qc_mask = in_time_qc_data[...] <= 2
    except KeyError:
        time_qc_mask = np.ones(in_time.shape, dtype=bool)

    try:
        in_position_qc = in_data.variables['POSITION_QC']
        if len(in_position_qc[...]) != len(in_time_data):
            if len(in_position_qc[...]) == 1:
                in_position_qc_data = np.ones(shape=in_time_data.shape) * in_position_qc[0]
            else:
                time.sleep(sleep_time)
                print(' Warning: position qc and time dimension variables have not the same size.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
        else:
            in_position_qc_data = in_position_qc[...]
        if np.ma.is_masked(in_position_qc_data):
            in_position_qc_data = in_position_qc_data.data
        in_position_qc_data = np.where(in_position_qc_data == 7, 2, in_position_qc_data)
        position_qc_mask = in_position_qc_data[...] <= 2
    except KeyError:
        position_qc_mask = np.ones(in_time.shape, dtype=bool)

    try:
        depth_dimension_name = 'DEPTH'
        depth_dimension_length = len(in_data.dimensions[depth_dimension_name])
    except KeyError:
        depth_dimension_length = 1

    good_time_mask = np.logical_and(time_qc_mask, position_qc_mask)
    in_time_data = in_time_data[good_time_mask]

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

    in_time_data = in_time_data[out_time_indices]

    # Retrieve variables that will be copied in output dataset
    in_longitude_variable_name = find_variable_name.find_variable_name(in_file, 'standard_name', 'longitude',
                                                                       verbose=False)
    in_longitude = in_data.variables[in_longitude_variable_name]
    try:
        in_longitude_data = in_longitude[good_time_mask]
    except IndexError:
        if len(in_longitude[...]) == 1:
            in_longitude_data = np.ones(shape=good_time_mask.shape) * in_longitude[0]
        else:
            time.sleep(sleep_time)
            print(' Warning: longitude and time dimension variables have not the same size.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return
    in_longitude_data = in_longitude_data[out_time_indices]
    in_latitude_variable_name = find_variable_name.find_variable_name(in_file, 'standard_name', 'latitude',
                                                                      verbose=False)
    in_latitude = in_data.variables[in_latitude_variable_name]
    try:
        in_latitude_data = in_latitude[good_time_mask]
    except IndexError:
        if len(in_latitude[...]) == 1:
            in_latitude_data = np.ones(shape=good_time_mask.shape) * in_latitude[0]
        else:
            time.sleep(sleep_time)
            print(' Warning: latitude and time dimension variables have not the same size.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return
    in_latitude_data = in_latitude_data[out_time_indices]

    in_depth = False
    try:
        in_depth_variable_name = find_variable_name.find_variable_name(in_file, 'standard_name', 'depth', verbose=False)
        in_depth = in_data.variables[in_depth_variable_name]
        in_depth_data = in_depth[good_time_mask]
        in_depth_data = in_depth_data[out_time_indices]
    except KeyError:
        pass
    if in_depth:
        if np.ma.is_masked(in_depth_data):
            in_depth_data = np.ma.array(np.where(in_depth_data.data == in_depth_data.fill_value, out_fill_value,
                                                 in_depth_data),
                                        mask=in_depth_data.mask, fill_value=out_fill_value, dtype=np.float32)
        else:
            in_depth_data = np.ma.array(in_depth_data, mask=np.zeros(shape=in_depth_data.shape, dtype=bool),
                                        fill_value=out_fill_value, dtype=np.float32)

        try:
            in_depth_qc = in_data.variables['DEPH_QC']
            in_depth_qc_data = in_depth_qc[good_time_mask]
            in_depth_qc_data = in_depth_qc_data[out_time_indices]
            if np.ma.is_masked(in_depth_qc_data):
                in_depth_qc_data = in_depth_qc_data.data
            in_depth_qc_data = np.where(in_depth_qc_data == 7, 2, in_depth_qc_data)
            in_depth_qc_mask = in_depth_qc_data[...] <= 2
            in_depth_data = np.ma.masked_where(np.invert(in_depth_qc_mask), in_depth_data)
        except KeyError:
            in_depth_qc = False
    else:
        in_depth_qc = False

    in_pres = False
    try:
        in_pres_variable_name = find_variable_name.find_variable_name(in_file, 'standard_name', 'sea_water_pressure',
                                                                      verbose=False)
        in_pres = in_data.variables[in_pres_variable_name]
        in_pres_data = in_pres[good_time_mask]
        in_pres_data = in_pres_data[out_time_indices]
    except KeyError:
        pass
    if in_pres:
        if np.ma.is_masked(in_pres_data):
            in_pres_data = np.ma.array(np.where(in_pres_data.data == in_pres_data.fill_value, out_fill_value,
                                                in_pres_data),
                                       mask=in_pres_data.mask, fill_value=out_fill_value, dtype=np.float32)
        else:
            in_pres_data = np.ma.array(in_pres_data, mask=np.zeros(shape=in_pres_data.shape, dtype=bool),
                                       fill_value=out_fill_value, dtype=np.float32)
        try:
            in_pres_qc = in_data.variables['PRES_QC']
            in_pres_qc_data = in_pres_qc[good_time_mask]
            in_pres_qc_data = in_pres_qc_data[out_time_indices]
            if np.ma.is_masked(in_pres_qc_data):
                in_pres_qc_data = in_pres_qc_data.data
            if np.array_equal(in_pres_qc_data, 7 * np.ones(in_pres_qc_data.shape)):
                in_pres_qc_mask = np.ones(in_pres_qc_data.shape, dtype=bool)
            else:
                in_pres_qc_mask = in_pres_qc_data[...] <= 2
            in_pres_data = np.ma.masked_where(np.invert(in_pres_qc_mask), in_pres_data)
        except KeyError:
            in_pres_qc = False
    else:
        in_pres_qc = False

    in_variable_name = find_variable_name.find_variable_name(in_file, 'standard_name', in_variable_standard_name,
                                                             verbose=False)

    if in_variable_name == 'PRRD':
        time.sleep(sleep_time)
        print(' Warning: CMEMS hourly and daily precipitation rate variables have the same standard name. '
              ' Removing the less frequent variable to avoid wrong units conversions...', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    in_variable = in_data.variables[in_variable_name]
    # Unset netCDF4-python valid_range mask
    in_variable.set_auto_mask(False)
    in_variable.set_auto_scale(False)
    try:
        in_variable_fill_value = in_variable._FillValue
        try:
            in_variable_scale_factor = in_variable.scale_factor
            in_variable_data = np.ma.array(in_variable[good_time_mask] * in_variable_scale_factor,
                                           mask=np.isclose(in_variable[good_time_mask], in_variable_fill_value))
        except AttributeError:
            in_variable_data = np.ma.array(in_variable[good_time_mask], mask=np.isclose(in_variable[good_time_mask],
                                                                                        in_variable_fill_value))
    except (AttributeError, TypeError):
        in_variable_data = in_variable[good_time_mask]
    in_variable_data = in_variable_data[out_time_indices]
    if np.ma.is_masked(in_variable_data):
        in_variable_data = np.ma.array(np.where(in_variable_data.data == in_variable_data.fill_value, out_fill_value,
                                                in_variable_data),
                                       mask=in_variable_data.mask, fill_value=out_fill_value,
                                       dtype=np.float32)
    else:
        in_variable_data = np.ma.array(in_variable_data, mask=np.zeros(shape=in_variable_data.shape, dtype=bool),
                                       fill_value=out_fill_value, dtype=np.float32)

    if in_depth_qc:
        in_variable_data = np.ma.masked_where(np.invert(in_depth_qc_mask), in_variable_data)
    if in_pres_qc:
        in_variable_data = np.ma.masked_where(np.invert(in_pres_qc_mask), in_variable_data)

    if in_variable_data.mask.all():
        time.sleep(sleep_time)
        print(' Warning: all data is missing in this variable.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    try:
        in_variable_qc = in_data.variables[in_variable_name + '_QC']
        in_variable_qc_data = in_variable_qc[good_time_mask]
        in_variable_qc_data = in_variable_qc_data[out_time_indices]
        if np.ma.is_masked(in_variable_qc_data):
            in_variable_qc_data = np.ma.array(np.where(in_variable_qc_data.data == in_variable_qc_data.fill_value,
                                                       out_fill_value, in_variable_qc_data),
                                              mask=in_variable_qc_data.mask, fill_value=out_fill_value,
                                              dtype=np.float32)
        else:
            in_variable_qc_data = np.ma.array(in_variable_qc_data, mask=np.zeros(shape=in_variable_qc_data.shape,
                                                                                 dtype=bool),
                                              fill_value=out_fill_value, dtype=np.float32)
        if in_depth_qc:
            in_variable_qc_data = np.ma.masked_where(np.invert(in_depth_qc_mask), in_variable_qc_data)
    except KeyError:
        in_variable_qc = False
        in_variable_qc_data = np.ma.array(np.ones(in_variable_data.shape),
                                          mask=np.zeros(shape=in_variable_data.shape, dtype=bool),
                                          fill_value=out_fill_value, dtype=np.float32)

    if verbose:
        print(' Removing time steps and depth levels with no data for original data.')
        # Remove time steps and depth levels with no data
    try:
        not_empty_mask = np.invert(np.ma.sum(in_variable_data, axis=1).mask)
    except np.AxisError:
        not_empty_mask = np.invert(in_variable_data.mask)
    not_empty_indices = np.where(not_empty_mask)[0]
    if not_empty_indices.shape[0] == 0:
        time.sleep(sleep_time)
        print(' Warning. No data for variable ' + in_variable_standard_name + ' in the selected period. Exiting.',
              file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    not_empty_shape = np.count_nonzero(not_empty_mask)

    out_time_data = in_time_data[not_empty_indices]
    if np.all(out_time_data.mask):
        time.sleep(sleep_time)
        print(' Warning. No valid time data in the selected period. Exiting.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    out_longitude_data = in_longitude_data[not_empty_indices]
    out_latitude_data = in_latitude_data[not_empty_indices]
    if in_depth:
        out_depth_data = np.ma.empty(shape=(not_empty_shape, 0),
                                     fill_value=in_depth_data.fill_value, dtype=in_depth_data.dtype)
    if in_pres:
        out_pres_data = np.ma.empty(shape=(not_empty_shape, 0),
                                    fill_value=in_pres_data.fill_value, dtype=in_pres_data.dtype)
    original_data = np.ma.empty(shape=(not_empty_shape, 0),
                                fill_value=in_variable_data.fill_value, dtype=in_variable_data.dtype)
    original_qc_data = np.ma.empty(shape=(not_empty_shape, 0),
                                   fill_value=in_variable_qc_data.fill_value,
                                   dtype=in_variable_qc_data.dtype)

    for depth in range(depth_dimension_length):
        try:
            original_data_depth_slice = np.ma.copy(in_variable_data[not_empty_indices][:, depth])
            not_filled_variable_data_depth_slice_number = original_data_depth_slice.shape[0] \
                - np.ma.count_masked(original_data_depth_slice)
            original_qc_data_depth_slice = np.array(in_variable_qc_data[not_empty_indices][:, depth],
                                                    dtype=in_variable_qc_data.dtype)
            if in_depth:
                out_depth_slice = np.ma.copy(in_depth_data[not_empty_indices][:, depth])
            if in_pres:
                out_pres_slice = np.ma.copy(in_pres_data[not_empty_indices][:, depth])
        except IndexError:
            original_data_depth_slice = np.ma.copy(in_variable_data[not_empty_indices])
            not_filled_variable_data_depth_slice_number = original_data_depth_slice.shape[0] \
                                                          - np.ma.count_masked(original_data_depth_slice)
            original_qc_data_depth_slice = np.array(in_variable_qc_data[not_empty_indices],
                                                    dtype=in_variable_qc_data.dtype)
            if in_depth:
                out_depth_slice = np.ma.copy(in_depth_data[not_empty_indices])
            if in_pres:
                out_pres_slice = np.ma.copy(in_pres_data[not_empty_indices])
        # if verbose:
        #     print(' Depth level ' + str(depth) + ':')
        #     print(str(not_filled_variable_data_depth_slice_number) + ' points of good data.')
        if not_filled_variable_data_depth_slice_number == 0:
            # if verbose:
            #     print(' Rejected...')
            continue
        else:
            # if verbose:
            #     print(' Accepted...')
            original_data = np.ma.append(original_data, original_data_depth_slice[:, np.newaxis], axis=1)
            original_qc_data = np.ma.append(original_qc_data, original_qc_data_depth_slice[:, np.newaxis], axis=1)
            if in_depth:
                out_depth_data = np.ma.append(out_depth_data, out_depth_slice[:, np.newaxis], axis=1)
            if in_pres:
                out_pres_data = np.ma.append(out_pres_data, out_pres_slice[:, np.newaxis], axis=1)

    if not in_depth:
        latitude_2d_data = np.transpose(np.repeat([out_latitude_data], out_pres_data.shape[-1], axis=0))
        out_depth_data = sw.dpth(out_pres_data, latitude_2d_data)

    if verbose:
        print(' Creating output dataset.')
    # Create output dataset
    out_data = netCDF4.Dataset(out_file, mode='w', format='NETCDF4')

    if verbose:
        print(' Creating dimensions.')
    # Create new dimensions

    out_data.createDimension('time', None)
    out_data.createDimension('depth', out_depth_data.shape[-1])

    if verbose:
        print(' Attaching dimension variables.')
    # Create new variables and set attributes
    out_longitude = out_data.createVariable('lon', datatype=np.float32,
                                            dimensions=('time',), zlib=True, complevel=1)
    out_longitude[...] = out_longitude_data
    out_longitude.long_name = 'Longitude of each location'
    out_longitude.standard_name = 'longitude'
    out_longitude.units = 'degree_east'
    out_longitude.axis = 'X'

    out_latitude = out_data.createVariable('lat', datatype=np.float32,
                                           dimensions=('time',), zlib=True, complevel=1)
    out_latitude[...] = out_latitude_data
    out_latitude.long_name = 'Latitude of each location'
    out_latitude.standard_name = 'latitude'
    out_latitude.units = 'degree_north'
    out_latitude.axis = 'Y'

    out_depth = out_data.createVariable('depth', datatype=np.float32, dimensions=('time', 'depth'),
                                        fill_value=out_fill_value, zlib=True, complevel=1)
    out_depth[...] = out_depth_data
    out_depth.positive = 'down'
    out_depth.long_name = 'Instantaneous depth'
    out_depth.standard_name = 'depth'
    out_depth.units = 'm'
    out_depth.axis = 'Z'
    if out_depth.shape[1] > 1:
        out_depth.valid_min = np.float32(np.ma.min(out_depth_data))
        out_depth.valid_max = np.float32(np.ma.max(out_depth_data))

    out_time = out_data.createVariable('time', datatype=np.float64, dimensions=('time',),
                                       zlib=True, complevel=1)
    out_time[...] = out_time_data + out_reference_data
    out_time.long_name = 'Time'
    out_time.standard_name = 'time'
    out_time.units = 'seconds since ' + out_time_reference
    out_time.calendar = 'gregorian'
    out_time.axis = 'T'

    # Create new variables and set attributes
    if verbose:
        print(' Creating ' + in_variable_standard_name + ' observed variable.')
    out_variable = out_data.createVariable(in_variable_standard_name, datatype=np.float32,
                                           dimensions=('time', 'depth'),
                                           fill_value=out_fill_value, zlib=True, complevel=1)
    out_variable[...] = original_data
    out_variable_attributes = [element for element in in_variable.ncattrs() if element not in '_FillValue']
    out_variable_attributes = [element for element in out_variable_attributes if element not in 'scale_factor']
    out_variable_attributes = [element for element in out_variable_attributes if element not in 'valid_min']
    out_variable_attributes = [element for element in out_variable_attributes if element not in 'valid_max']
    out_variable.setncatts({attr: in_variable.getncattr(attr) for attr in out_variable_attributes})
    out_variable.valid_min = np.min(original_data)
    out_variable.valid_max = np.max(original_data)

    if verbose:
        print(' Creating ' + in_variable_standard_name + ' observed qc variable.')
    out_variable_qc = out_data.createVariable(in_variable_standard_name + '_qc', datatype=np.float32,
                                              dimensions=('time', 'depth'),
                                              fill_value=out_fill_value, zlib=True, complevel=1)
    out_variable_qc[...] = original_qc_data
    if in_variable_qc:
        out_variable_qc_attributes = [element for element in in_variable_qc.ncattrs() if element not in '_FillValue']
        out_variable_qc.setncatts({attr: in_variable_qc.getncattr(attr) for attr in out_variable_qc_attributes})
    out_variable_qc.valid_min = np.min(original_qc_data)
    out_variable_qc.valid_max = np.max(original_qc_data)

    if verbose:
        print(' Setting global attributes.')
    # Set global attributes
    global_attributes = [element for element in in_data.ncattrs() if element not in 'comment']
    global_attributes = [element for element in global_attributes if element not in 'summary']
    global_attributes = [element for element in global_attributes if not element.startswith('date')]
    global_attributes = [element for element in global_attributes if not element.startswith('history')]
    global_attributes = [element for element in global_attributes if not element.startswith('quality')]
    global_attributes = [element for element in global_attributes if not element.startswith('qc')]
    global_attributes = [element for element in global_attributes if not element.startswith('geospatial')]
    global_attributes = [element for element in global_attributes if not element.startswith('update')]
    global_attributes = [element for element in global_attributes if not element.startswith('time')]
    global_attributes = [element for element in global_attributes if not element.startswith('last')]

    out_data.setncatts({attr: in_data.getncattr(attr) for attr in global_attributes})

    try:
        out_data.history = in_data.history
    except AttributeError:
        out_data.history = ''
    out_data.history = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + \
        ' : Cut record dimension and removed unwanted dimensions and variables\n' + out_data.history
    out_data.history = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) +\
        ' : Removed depth levels and time records with no data\n' + out_data.history
    out_data.source = 'SOURCE evaluation system'
    out_data.SOURCE_platform_code = in_data.platform_code

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
        in_variable_standard_name = sys.argv[2]
        out_file = sys.argv[3]

    except (IndexError, ValueError):
        in_file = None
        in_variable_standard_name = None
        out_file = None

    try:
        time.strptime(sys.argv[4], '%Y%m%d')
        first_date_str = sys.argv[4]
    except (IndexError, ValueError):
        try:
            time.strptime(sys.argv[4], '%Y-%m-%d %H:%M:%S')
            first_date_str = sys.argv[4]
        except (IndexError, ValueError):
            first_date_str = None

    try:
        time.strptime(sys.argv[5], '%Y%m%d')
        last_date_str = sys.argv[5]
    except (IndexError, ValueError):
        try:
            time.strptime(sys.argv[5], '%Y-%m-%d %H:%M:%S')
            last_date_str = sys.argv[5]
        except (IndexError, ValueError):
            last_date_str = None

    try:
        verbose = string_to_bool(sys.argv[6])
    except (IndexError, ValueError):
        verbose = True

    insitu_tac_timeseries_extractor(in_file, in_variable_standard_name, out_file,
                                    first_date_str, last_date_str, verbose)
