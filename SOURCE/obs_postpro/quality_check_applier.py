# -*- coding: utf-8 -*-
import sys
import os
import numpy as np
import netCDF4
import time
from SOURCE import find_variable_name

# Global variables
sleep_time = 0.1  # seconds
out_fill_value = 1.e20


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def quality_check_applier(in_file=None, in_variable_standard_name=None, valid_qc_values=None, out_file=None,
                          iteration=-1, verbose=True):
    if __name__ == '__main__':
        return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to apply a specific quality check to a variable stored in a netCDF dataset.')
        print(' -------------------------')
    if in_file is None or in_variable_standard_name is None or valid_qc_values is None or out_file is None:
        time.sleep(sleep_time)
        print(' Error: 4 of 6 maximum arguments (2 optionals) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) input variable standard_name;', file=sys.stderr)
        print(' 3) input variable valid qc values to consider (spaced valued string, example: "0 1 2");',
              file=sys.stderr)
        print(' 4) output file;', file=sys.stderr)
        print(' 5) (optional) post processed datasets iteration number (default: -1)')
        print(' 6) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    if verbose:
        print(' Input file = ' + in_file)
        print(' Input 1D or 2D variable standard_name = ' + in_variable_standard_name)
        print(' Valid qc values to consider = ' + valid_qc_values)
        print(' Output file = ' + out_file)
        if iteration != -1:
            print(' Post processing procedure iterations number = ' + str(iteration))
        print(' Verbosity switch = ' + str(verbose))
        print(' -------------------------')

    try:
        valid_qc_values = [int(valid_qc_value) for valid_qc_value in valid_qc_values.split(' ')]
    except ValueError:
        time.sleep(sleep_time)
        print(' Error. Wrong valid qc values string.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    if verbose:
        print(' Opening input dataset.')
    # Open input dataset
    in_data = netCDF4.Dataset(in_file, mode='r')
    in_variable_name = find_variable_name.find_variable_name(in_file, 'standard_name', in_variable_standard_name,
                                                             verbose=False)
    try:
        in_variable = in_data.variables[in_variable_name]
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

    try:
        in_variable_qc = in_data.variables[in_variable_name + '_QC']
        in_variable_qc_name = in_variable_name + '_QC'
    except KeyError:
        try:
            in_variable_qc = in_data.variables[in_variable_name + '_qc']
            in_variable_qc_name = in_variable_name + '_qc'
        except KeyError:
            time.sleep(sleep_time)
            print(' Error. Quality check variable is not present in input dataset.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return

    if ('time' not in in_data.variables[in_variable_qc_name].dimensions) or\
            (len(in_data.variables[in_variable_qc_name].shape) < 1):
        time.sleep(sleep_time)
        print(' Error. Quality check variable is not 1D, 2D or does not contain a record dimension.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    if in_data.variables[in_variable_name].shape != in_data.variables[in_variable_qc_name].shape:
        if (in_data.variables[in_variable_qc_name].shape[: -1] == in_data.variables[in_variable_name].shape) \
                and ('iteration' in in_data.variables[in_variable_qc_name].dimensions):
            pass
        else:
            time.sleep(sleep_time)
            print(' Error. Input variable and quality check variable dimensions mismatch.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return

    print(' Applying QC to selected variable.')
    # Compute selected variable average
    in_variable_data = in_variable[...]
    if not np.ma.is_masked(in_variable_data):
        in_variable_data = np.ma.array(in_variable_data, mask=np.zeros(shape=in_variable_data.shape, dtype=bool),
                                       fill_value=out_fill_value, dtype=in_variable_data.dtype)
    if in_variable_data.mask.all():
        time.sleep(sleep_time)
        print(' Warning: all data is missing in this variable.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    out_variable_data = np.ma.copy(in_variable_data)
    if (in_data.variables[in_variable_qc_name].shape[: -1] == in_data.variables[in_variable_name].shape) \
            and ('iteration' in in_data.variables[in_variable_qc_name].dimensions):
        try:
            in_variable_qc_data = in_variable_qc[..., iteration]
        except IndexError:
            in_variable_qc_data = in_variable_qc[..., 1]
    else:
        in_variable_qc_data = in_variable_qc[...]

    valid_data_mask = np.zeros(in_variable_qc_data.shape, dtype=bool)
    for valid_qc_value in valid_qc_values:
        valid_data_mask = np.logical_or(valid_data_mask, in_variable_qc_data == valid_qc_value)
    out_variable_data.mask[np.invert(valid_data_mask)] = True

    if out_variable_data.mask.all():
        time.sleep(sleep_time)
        print(' Warning: all data is missing when applying this quality check.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    if verbose:
        print(' Depth indices: ' + str(in_data.dimensions['depth'].size))

    # Retrieve time variable

    if verbose:
        print(' Starting process...')
        print(' -------------------------')

    if verbose:
        print(' Creating output dataset.')
    # Create output dataset
    out_data = netCDF4.Dataset(out_file, mode='w', format='NETCDF4')

    if verbose:
        print(' Creating dimensions.')
    # Create new dimensions
    for dimension_name in in_data.dimensions:
        if dimension_name in ['month', 'iteration', 'samples', 'axis_nbounds']:
            continue
        out_data.createDimension(dimension_name, in_data.dimensions[dimension_name].size
                                 if not in_data.dimensions[dimension_name].isunlimited() else None)

    if verbose:
        print(' Creating variables.')

    out_dimension_variables = ['lon', 'lat', 'depth', 'time']

    for dimension_variable_name in out_dimension_variables:
        in_dimension_variable = in_data.variables[dimension_variable_name]
        if verbose:
            print(' Attaching variable ' + dimension_variable_name)
        if dimension_variable_name == 'time':
            out_dimension_variable = out_data.createVariable(dimension_variable_name, in_dimension_variable.datatype,
                                                             dimensions=in_dimension_variable.dimensions,
                                                             fill_value=out_fill_value, zlib=True, complevel=1)
        elif 'time' in in_dimension_variable.dimensions:
            out_dimension_variable = out_data.createVariable(dimension_variable_name, in_dimension_variable.datatype,
                                                             dimensions=in_dimension_variable.dimensions,
                                                             fill_value=out_fill_value, zlib=True, complevel=1)
        else:
            out_dimension_variable = out_data.createVariable(dimension_variable_name, in_dimension_variable.datatype,
                                                             dimensions=in_dimension_variable.dimensions)
        out_dimension_variable[...] = in_dimension_variable[...]
        variable_attributes = [attribute for attribute in in_dimension_variable.ncattrs()
                               if attribute not in '_FillValue']
        out_dimension_variable.setncatts({attribute: in_dimension_variable.getncattr(attribute)
                                         for attribute in variable_attributes})

    print(' Attaching variable ' + in_variable_name)
    # Writing output quality checked variable
    out_variable = out_data.createVariable(in_variable_name, in_variable.datatype,
                                           dimensions=in_variable.dimensions,
                                           fill_value=out_fill_value, zlib=True, complevel=1)
    variable_attributes = [attribute for attribute in in_variable.ncattrs() if attribute not in '_FillValue']
    out_variable.setncatts({attribute: in_variable.getncattr(attribute) for attribute in variable_attributes})
    out_variable[...] = out_variable_data

    if verbose:
        print(' Setting global attributes.')
    # Set global attributes
    global_attributes = [element for element in in_data.ncattrs() if not element.startswith('history')]
    out_data.setncatts({attribute: in_data.getncattr(attribute) for attribute in global_attributes})

    out_data.history = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) +\
        ' : Insitu quality check ' + in_variable_qc_name + ' with values ' +\
        ' '.join(list(map(str, valid_qc_values))) + ' applied\n' + in_data.history

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
        valid_qc_values = sys.argv[3]
        out_file = sys.argv[4]

    except (IndexError, ValueError):
        in_file = None
        in_variable_standard_name = None
        valid_qc_values = None
        out_file = None

    try:
        iteration = int(sys.argv[4])
    except (IndexError, ValueError):
        iteration = -1

    try:
        verbose = string_to_bool(sys.argv[5])
    except (IndexError, ValueError):
        verbose = True

    quality_check_applier(in_file, in_variable_standard_name, valid_qc_values, out_file, iteration, verbose)
