# -*- coding: utf-8 -*-
import sys
import os
import time
import netCDF4

# Global variables
sleep_time = 0.1  # seconds


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def find_variable_name(in_file=None, in_variable_attribute_name=None, in_variable_attribute_value=None, verbose=True):
    # if __name__ == '__main__':
    #     return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to find variable name given an attribute name and value.')
        print(' -------------------------')
    if in_file is None or in_variable_attribute_name is None or in_variable_attribute_value is None:
        time.sleep(sleep_time)
        print(' ERROR: 2 of 4 maximum arguments (2 optionals) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) input variable attribute name;', file=sys.stderr)
        print(' 3) input variable attribute value;', file=sys.stderr)
        print(' 3) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    if verbose:
        print(' Input file = ' + in_file)
        print(' Input variable attribute name = ' + in_variable_attribute_name)
        print(' Input variable attribute value = ' + in_variable_attribute_value)
        print(' Verbosity switch = ' + str(verbose))
        print(' -------------------------')

    in_data = netCDF4.Dataset(in_file, mode='r')

    in_variable_name = None
    for variable in in_data.variables.keys():
        try:
            variable_attribute = getattr(in_data.variables[variable], in_variable_attribute_name)
        except AttributeError:
            continue
        if variable_attribute == in_variable_attribute_value:
            in_variable_name = variable
            break

    if in_variable_name is None:
        if verbose:
            time.sleep(sleep_time)
            print(' No variable name with "' + in_variable_attribute_name +
                  '" valued "' + in_variable_attribute_value + '" standard_name found.', file=sys.stderr)
            time.sleep(sleep_time)
        if os.path.basename(sys.argv[0]) == __file__:
            return
        else:
            raise KeyError(in_variable_attribute_name)

    if verbose:
        print(' Variable name of ' + in_variable_attribute_value + ' = ' + str(in_variable_name))

    in_data.close()

    return in_variable_name


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_file = sys.argv[1]
        in_variable_attribute_name = sys.argv[2]
        in_variable_attribute_value = sys.argv[3]

    except (IndexError, ValueError):
        in_file = None
        in_variable_attribute_name = None
        in_variable_attribute_value = None

    try:
        verbose = string_to_bool(sys.argv[4])
    except (IndexError, ValueError):
        verbose = True

    in_variable_name = find_variable_name(in_file, in_variable_attribute_name, in_variable_attribute_value, verbose)
    print(in_variable_name)
