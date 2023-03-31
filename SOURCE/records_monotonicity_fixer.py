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
def records_monotonicity_fixer(in_file=None, out_file=None, verbose=True):
    # if __name__ == '__main__':
    #     return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to reorder decreasing records segments in netCDF files.')
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
    sort_indices = np.argsort(in_time_data)

    if np.all(np.diff(in_time_data) > 0):
        if verbose:
            print(' No reversed time step segments found. Copying input file to output file and exiting.')
            print(' -------------------------')
        in_data.close()
        shutil.copy2(in_file, out_file)
        return

    if verbose:
        print(' Starting process...')
        print(' -------------------------')
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
        if ('time' in in_variable.dimensions) and verbose:
            print(' Fixing ' + variable_name + ' monotonicity')
        in_variable_data = in_variable[...]
        if (variable_name == 'time') or (variable_name == 'time_bounds'):
            out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                   dimensions=in_variable.dimensions)
            out_variable[...] = in_variable_data[sort_indices]
        elif 'time' in in_variable.dimensions:
            out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                   dimensions=in_variable.dimensions,
                                                   fill_value=out_fill_value, zlib=True, complevel=1)
            out_variable[...] = in_variable_data[sort_indices]
        else:
            out_variable = out_data.createVariable(variable_name, in_variable.datatype,
                                                   dimensions=in_variable.dimensions)
            out_variable[...] = in_variable_data
        variable_attributes = [attribute for attribute in in_variable.ncattrs() if attribute not in '_FillValue']
        out_variable.setncatts({attribute: in_variable.getncattr(attribute) for attribute in variable_attributes})

    if verbose:
        print(' Setting global attributes.')
    # Set global attributes
    global_attributes = [element for element in in_data.ncattrs() if not element.startswith('history')]
    out_data.setncatts({attribute: in_data.getncattr(attribute) for attribute in global_attributes})
    out_data.history = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + \
        ' : Fixed monotonicity in time range\n' + in_data.history
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

    records_monotonicity_fixer(in_file, out_file, verbose)
