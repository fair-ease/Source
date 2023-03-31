# -*- coding: utf-8 -*-
import sys
import os
import shutil
import time
import numpy as np
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
def duplicated_records_remover(in_file=None, out_file=None, verbose=True):
    # if __name__ == '__main__':
    #     return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to remove duplicated records in netCDF files.')
        print(' -------------------------')
    if in_file is None or out_file is None:
        time.sleep(sleep_time)
        print(' Error: 2 of 3 maximum arguments (1 optional) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) output file;', file=sys.stderr)
        print(' 3) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    if verbose:
        print(' Input file = ' + in_file)
        print(' Output file = ' + out_file)
        print(' Verbosity switch = ' + str(verbose))

    in_data = netCDF4.Dataset(in_file, mode='r')
    in_time_data = in_data.variables['time'][:]

    [out_time_data, unique_indices, reverse_indices, duplicated_counts] = \
        np.unique(in_time_data, return_inverse=True, return_index=True, return_counts=True)
    duplicated_indices = np.unique(list(set(np.arange(in_time_data.shape[0])) - set(unique_indices)))

    if duplicated_indices.shape[0] == 0:
        if verbose:
            print(' No Duplicated time steps found. Copying input file to output file and exiting.')
            print(' -------------------------')
        in_data.close()
        shutil.copy2(in_file, out_file)
        return

    if verbose:
        print(' Starting process...')
        print(' -------------------------')

        print(' Duplicated time steps: ' + str(duplicated_indices.shape[0]))

    if verbose:
        print(' Creating output dataset.')
    # Create output dataset
    out_data = netCDF4.Dataset(out_file, mode='w', format='NETCDF4')

    # Copy fixed dimensions in output dataset
    if verbose:
        print(' Creating dimensions.')
    for dimension_name in in_data.dimensions.keys():
        out_data.createDimension(dimension_name, in_data.dimensions[dimension_name].size
                                 if not in_data.dimensions[dimension_name].isunlimited() else None)

    # Copy fixed variables in output dataset
    for variable_name in in_data.variables.keys():
        in_variable = in_data.variables[variable_name]
        if 'time' in in_variable.dimensions and verbose:
            print(' Removing duplicated time steps for variable ' + variable_name)
        in_variable_data = in_variable[...]
        if not np.ma.is_masked(in_variable_data):
            in_variable_data = np.ma.array(in_variable_data, mask=np.zeros(shape=in_variable_data.shape, dtype=bool),
                                           fill_value=out_fill_value, dtype=in_variable.datatype)
        if variable_name == 'time':
            out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                   dimensions=in_variable.dimensions)
            out_variable[...] = out_time_data
        elif 'time' in in_variable.dimensions:
            in_variable_filled_data = np.ma.filled(in_variable_data, fill_value=0.)
            out_variable_sum_data = np.zeros(shape=(out_time_data.shape[0],) + in_variable_data.shape[1:])
            np.add.at(out_variable_sum_data, reverse_indices, in_variable_filled_data)
            out_variable_masked_count = np.zeros(
                shape=(out_time_data.shape[0],) + in_variable_data.shape[1:], dtype=int)
            np.add.at(out_variable_masked_count, reverse_indices, in_variable_data.mask)
            out_variable_count_data = np.copy(duplicated_counts)
            for dim in range(1, in_variable_data.ndim):
                out_variable_count_data = \
                    np.repeat(out_variable_count_data[..., np.newaxis], in_variable_data.shape[dim], axis=-1)
            out_variable_count_data -= out_variable_masked_count
            out_variable_averaged_mask = np.array(
                np.where(out_variable_count_data == 0, 1, 0),
                dtype=bool)
            out_variable_count_data = np.where(out_variable_count_data == 0, 1, out_variable_count_data)
            out_variable_averaged_data = np.divide(out_variable_sum_data, out_variable_count_data)
            out_variable_averaged_data = np.ma.array(out_variable_averaged_data, mask=out_variable_averaged_mask,
                                                     fill_value=out_fill_value, dtype=out_variable_averaged_data.dtype)
            if variable_name == 'time_bounds':
                out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                       dimensions=in_variable.dimensions)
            else:
                out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                       dimensions=in_variable.dimensions,
                                                       fill_value=out_fill_value,
                                                       zlib=True, complevel=1)
            out_variable[...] = out_variable_averaged_data
        else:
            out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                   dimensions=in_variable.dimensions)
            out_variable[...] = in_variable[...]
        variable_attributes = [attribute for attribute in in_variable.ncattrs() if attribute not in '_FillValue']
        out_variable.setncatts({attribute: in_variable.getncattr(attribute) for attribute in variable_attributes})

    if verbose:
        print(' Setting global attributes.')
    # Set global attributes
    global_attributes = [element for element in in_data.ncattrs() if not element.startswith('history')]
    out_data.setncatts({attribute: in_data.getncattr(attribute) for attribute in global_attributes})
    out_data.history = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + \
        ' : Removed duplicated time steps\n' + in_data.history

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
        out_file = sys.argv[2]

    except (IndexError, ValueError):
        in_file = None
        out_file = None

    try:
        verbose = string_to_bool(sys.argv[3])
    except (IndexError, ValueError):
        verbose = True

    duplicated_records_remover(in_file, out_file, verbose)
