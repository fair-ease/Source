# -*- coding: utf-8 -*-
import sys
import os
import shutil
import numpy as np
import netCDF4
import time
import calendar
from SOURCE import time_calc


# Global variables
sleep_time = 0.1  # seconds
out_fill_value = 1.e20


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


def intersect(string_list):
    intersection = string_list[0]
    for index in range(len(string_list)):
        string_intersection = \
            [''.join([character for character in intersection if character in string_list[element]])
             for element in range(index, len(string_list))]
        intersection = min(string_intersection, key=len)
    return intersection


# Functional version
def pointwise_datasets_concatenator(in_list=None, out_file=None, in_fields_standard_name_str=None,
                                    first_date_str=None, last_date_str=None, verbose=True):
    """
    Script to concatenate pointwise devices dataset with preserving timestep ordering

    Input arguments:

        1) Insitu devices datasets list;

        2) Output concatenated dataset;

        3) Input variables standard_name attributes to process space separated string
            (for example: "sea_water_temperature sea_water_practical_salinity", please read CF conventions standard name
            table to find the correct strings to insert);

        4) First date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format (OPTIONAL);

        5) Last date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS (OPTIONAL);

        6) Verbosity switch (OPTIONAL)

    Written Nov 9, 2017 by Paolo Oliveri
    """
    if __name__ == '__main__':
        return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Pointwise datasets concatenator.')
        print(' -------------------------')
    if in_list is None or out_file is None:
        time.sleep(sleep_time)
        print(' ERROR: 2 of 6 maximum arguments (4 optionals) not provided.', file=sys.stderr)
        print(' 1) Input datasets list;', file=sys.stderr)
        print(' 2) Output concatenated dataset file name;', file=sys.stderr)
        print(' 3) (optional) Input fields standard_name space separated string'
              ' (default: merge all variables that are present in each file of the list);', file=sys.stderr)
        print(' 4) (optional) First date to evaluate in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: first recorded date for each device);', file=sys.stderr)
        print(' 5) (optional) Last date to evaluate in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: last recorded date for each device);', file=sys.stderr)
        print(' 6) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    try:
        in_list = in_list.split(' ')
    except AttributeError:
        pass
    file_list = [element for element in in_list if element.endswith('.nc')]
    if not file_list:
        file_list = [in_list[0] + '/' + in_file for in_file in os.listdir(in_list[0]) if in_file.endswith('.nc')]
        if not file_list:
            time.sleep(sleep_time)
            print(' Error. No processable files in input directory.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return
    if verbose:
        print(' Input list = ' + ', '.join(file_list))
        print(' Output concatenated dataset file name = ' + out_file)
        print(' Input variables to process standard_name string = ' + str(in_fields_standard_name_str) +
              ' (if None all variables that are present in each file of the list will be processed)')
        print(' First date to process = ' + str(first_date_str) +
              ' (if None it will be the first available date on each device)')
        print(' Last date to process = ' + str(last_date_str) +
              ' (if None it will be the last available date on each device)')
        print(' verbosity switch = ' + str(verbose))
        print(' -------------------------')
        print(' Starting process...')
        print(' -------------------------')
    if (in_fields_standard_name_str == '') or (in_fields_standard_name_str == 'None'):
        in_fields_standard_name_str = None

    if in_fields_standard_name_str is not None:
        in_fields_standard_name_list = in_fields_standard_name_str.split(' ')
        if not in_fields_standard_name_list or (in_fields_standard_name_list == list()):
            time.sleep(sleep_time)
            print(' Error: wrong or empty input variables string.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return

    if first_date_str is not None:
        try:
            first_date = time.strptime(first_date_str, '%Y%m%d')
        except ValueError:
            first_date = time.strptime(first_date_str, '%Y-%m-%d %H:%M:%S')

    if last_date_str is not None:
        try:
            last_date = time.strptime(last_date_str, '%Y%m%d')
        except ValueError:
            last_date = time.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')

    if (first_date_str is not None) and (last_date_str is not None):
        if first_date > last_date:
            time.sleep(sleep_time)
            print(' Error: selected first date is greater than last date. Exiting.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return
    if first_date_str is not None:
        first_date_seconds = calendar.timegm(first_date)
    if last_date_str is not None:
        last_date_seconds = calendar.timegm(last_date)
    output_variables = list()
    time_starts = np.empty(shape=0)
    time_ends = np.empty(shape=0)
    time_steps = np.empty(shape=0)
    time_dimensions = np.empty(shape=0, dtype=int)
    depth_dimensions = np.empty(shape=0, dtype=int)
    variables_number = np.empty(shape=0, dtype=int)
    first_file = True
    file_cut_list = list()
    for file_number in range(len(file_list)):
        in_file = file_list[file_number]
        in_data = netCDF4.Dataset(in_file, mode='r')
        try:
            in_data.dimensions['LONGITUDE'].size
        except KeyError:
            try:
                in_data.dimensions['lon'].size
            except KeyError:
                pass
        try:
            longitude_variable_name = in_data.variables['LONGITUDE'].name
        except KeyError:
            try:
                longitude_variable_name = in_data.variables['lon'].name
            except KeyError:
                time.sleep(sleep_time)
                print(' Error: missing longitude variable.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
        try:
            in_data.dimensions['LATITUDE'].size
        except KeyError:
            try:
                in_data.dimensions['lat'].size
            except KeyError:
                pass
        try:
            latitude_variable_name = in_data.variables['LATITUDE'].name
        except KeyError:
            try:
                latitude_variable_name = in_data.variables['lat'].name
            except KeyError:
                time.sleep(sleep_time)
                print(' Error: missing latitude variable.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
        try:
            in_depth_dimension = in_data.dimensions['DEPTH'].size
            depth_dimension_name = 'DEPTH'
        except KeyError:
            try:
                in_depth_dimension = in_data.dimensions['depth'].size
                depth_dimension_name = 'depth'
            except KeyError:
                in_depth_dimension = 1.
                depth_dimension_name = 'DEPTH'
        try:
            depth_variable_name = in_data.variables['DEPH'].name
        except KeyError:
            try:
                depth_variable_name = in_data.variables['depth'].name
            except KeyError:
                depth_variable_name = ''
        try:
            pres_variable_name = in_data.variables['PRES'].name
        except KeyError:
            try:
                pres_variable_name = in_data.variables['pres'].name
            except KeyError:
                pres_variable_name = ''
        try:
            in_time_dimension = in_data.dimensions['TIME'].size
            time_dimension_name = 'TIME'
        except KeyError:
            try:
                in_time_dimension = in_data.dimensions['time'].size
                time_dimension_name = 'time'
            except KeyError:
                time.sleep(sleep_time)
                print(' Error: missing record dimension.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
        if in_time_dimension == 1:
            continue
        try:
            in_time = in_data.variables['TIME']
            time_variable_name = 'TIME'
        except KeyError:
            try:
                in_time = in_data.variables['time']
                time_variable_name = 'time'
            except KeyError:
                time.sleep(sleep_time)
                print(' Error: missing record variable.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return
        in_time_reference = in_time.units
        if 'days' in in_time_reference:
            in_time_data = np.round(in_time[...] * 86400.)
        elif 'seconds' in in_time_reference:
            in_time_data = np.round(in_time[...])
        in_time_reference = in_time_reference[in_time_reference.find('since ') + len('since '):]
        try:
            in_reference_data = abs(calendar.timegm(time.strptime(in_time_reference, '%Y-%m-%dT%H:%M:%SZ')))
        except ValueError:
            in_reference_data = abs(calendar.timegm(time.strptime(in_time_reference, '%Y-%m-%d %H:%M:%S')))
        in_time_data += - in_reference_data
        start_time = np.min(in_time_data)
        end_time = np.max(in_time_data)

        if first_date_str is not None:
            if end_time < first_date_seconds:
                in_data.close()
                continue
        if last_date_str is not None:
            if start_time > last_date_seconds:
                in_data.close()
                continue
        if in_fields_standard_name_str is not None:
            try:
                position_variable = in_data.variables['POSITIONING_SYSTEM']
                position_variable_name = position_variable.name[:8]
            except KeyError:
                position_variable_name = ''
            try:
                position_qc_variable_name = in_data.variables['POSITION_QC'].name
            except KeyError:
                position_qc_variable_name = ''
            try:
                dc_reference_variable_name = in_data.variables['DC_REFERENCE'].name
            except KeyError:
                dc_reference_variable_name = ''
            dimension_variables_list = \
                [longitude_variable_name, latitude_variable_name, depth_variable_name, pres_variable_name,
                 time_variable_name, position_variable_name, dc_reference_variable_name, position_qc_variable_name]
            dimension_variables_list = [variable for variable in dimension_variables_list if variable != '']
            in_variables_list = list()
            for variable in in_data.variables.keys():
                try:
                    in_data.variables[variable].standard_name
                except AttributeError:
                    continue
                if in_data.variables[variable].standard_name in in_fields_standard_name_list:
                    in_variables_list.append(variable)
            test_variables = in_variables_list + dimension_variables_list
        else:
            test_variables = in_data.variables.keys()
        file_cut_list.append(in_file)
        if first_file:
            first_file = False
            output_dimensions = list(in_data.dimensions.keys())
            output_dimensions_sizes = \
                [in_data.dimensions[dimension_name].size for dimension_name in output_dimensions]
            output_variables += [variable for variable in in_data.variables.keys()
                                 if [test_variable for test_variable in test_variables if test_variable in variable]]
        else:
            output_dimensions_sizes +=\
                [in_data.dimensions[dimension_name].size for dimension_name in in_data.dimensions.keys()
                 if dimension_name not in output_dimensions]
            output_dimensions +=\
                [dimension for dimension in in_data.dimensions.keys() if dimension not in output_dimensions]
            output_variables +=\
                [variable for variable in in_data.variables.keys() if (variable not in output_variables)
                 and [test_variable for test_variable in test_variables if test_variable in variable]]
        time_dimensions = np.append(time_dimensions, in_time_dimension)
        depth_dimensions = np.append(depth_dimensions, in_depth_dimension)
        time_starts = np.append(time_starts, start_time)
        time_ends = np.append(time_ends, end_time)
        variables_number = np.append(variables_number, len(in_data.variables.keys()))
        in_data.close()
        time_step = time_calc.time_calc(in_file, verbose=False)
        time_steps = np.append(time_steps, time_step)

    if np.unique(depth_dimensions).shape[0] > 1:
        time.sleep(sleep_time)
        print(' Warning: depth dimension is not the same for every file. Trying to self-adjust.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')

    if not file_cut_list:
        time.sleep(sleep_time)
        print(' Warning: no datasets to concatenate for selected time period.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    sort_indices = np.argsort(time_starts)
    time_starts = time_starts[sort_indices]
    time_ends = time_ends[sort_indices]
    variables_number = variables_number[sort_indices]
    time_steps = time_steps[sort_indices]
    time_dimensions = time_dimensions[sort_indices]
    depth_dimensions = depth_dimensions[sort_indices]
    time_sorted_list = [file_cut_list[file_number] for file_number in sort_indices]

    # noinspection PyUnusedLocal
    time_interceptions = [list() for index in range(len(time_starts))]
    for first_index in range(len(time_starts)):
        for second_index in range(len(time_starts)):
            if second_index == first_index:
                continue
            if (time_starts[second_index] < time_ends[first_index]) and \
                    (time_ends[second_index] > time_starts[first_index]):
                time_interceptions[first_index].append(second_index)

    out_variables = [variable for variable in output_variables if variable not in [time_variable_name]]
    work_time_data = {}
    work_time_seconds = {}
    work_variable_data = {}
    work_variable_datatypes = {}
    work_variable_dimensions = {}
    work_variable_fill_value = {}
    variable_attributes = {}
    out_depth_dimension = np.max(depth_dimensions)

    if len(time_sorted_list) == 1:
        if verbose:
            print(' Warning. There is only one file of the list matching the concatenation requests:', file=sys.stderr)
            print(time_sorted_list[0] + '. Copying it to the output directory.', file=sys.stderr)
            shutil.copy2(time_sorted_list[0], out_file)
    else:
        first_file = True
        for in_index in range(len(time_sorted_list)):
            in_file = time_sorted_list[in_index]
            if verbose:
                print(' Opening dataset ' + in_file + '.')
            in_data = netCDF4.Dataset(in_file, mode='r')
            in_time = in_data.variables[time_variable_name]

            if first_file:
                out_time_datatype = in_time.datatype
                try:
                    time_fill_value = in_data.variables[time_variable_name]._FillValue
                except AttributeError:
                    time_fill_value = None
                out_time_data = np.ma.empty(shape=(0,), fill_value=time_fill_value, dtype=in_time.datatype)
                time_attributes = \
                    {attr: in_time.getncattr(attr) for attr in in_time.ncattrs() if attr not in '_FillValue'}
                if ('valid_min' in time_attributes.keys()) and ('valid_max' in time_attributes.keys()):
                    if time_attributes['valid_min'] != -time_attributes['valid_max']:
                        if verbose:
                            time.sleep(sleep_time)
                            print(' Warning: time valid_min seems different from the opposite of valid_max.'
                                  ' Correcting it accordingly.', file=sys.stderr)
                            time.sleep(sleep_time)
                        time_attributes['valid_min'] = -time_attributes['valid_max']
                global_attributes = {attr: in_data.getncattr(attr) for attr in in_data.ncattrs()}

            if np.ma.is_masked(in_time_data):
                if all(in_time_data.mask):
                    in_time_data = in_time[...].data
                else:
                    in_time_data = in_time[...]
            else:
                in_time_data = in_time[...]

            in_time_reference = in_time.units
            if 'days' in in_time_reference:
                in_time_seconds = np.array(np.round(in_time_data * 86400.), dtype=np.int64)
            elif 'seconds' in in_time_reference:
                in_time_seconds = np.array(np.round(in_time_data), dtype=np.int64)
            in_time_reference = in_time_reference[in_time_reference.find('since ') + len('since '):]
            try:
                in_reference_data = abs(calendar.timegm(time.strptime(in_time_reference, '%Y-%m-%dT%H:%M:%SZ')))
            except ValueError:
                in_reference_data = abs(calendar.timegm(time.strptime(in_time_reference, '%Y-%m-%d %H:%M:%S')))
            in_time_seconds += - in_reference_data

            work_time_seconds[in_index] = in_time_seconds
            work_time_data[in_index] = in_time_data
            work_variable_data[in_index] = {}
            work_variable_datatypes[in_index] = {}
            work_variable_dimensions[in_index] = {}
            work_variable_fill_value[in_index] = {}
            for variable_name in out_variables:
                variable_presence = True
                try:
                    in_variable = in_data.variables[variable_name]
                    if verbose:
                        print(' Extracting variable ' + variable_name + '.')
                    in_variable.set_auto_mask(False)
                    in_variable.set_auto_scale(False)
                    try:
                        in_variable_fill_value = in_variable._FillValue
                        try:
                            in_variable_scale_factor = in_variable.scale_factor
                            in_variable_data = np.ma.array(in_variable[...] * in_variable_scale_factor,
                                                           mask=np.isclose(in_variable[...], in_variable_fill_value))
                        except AttributeError:
                            in_variable_data = np.ma.array(in_variable[...], mask=np.isclose(in_variable[...],
                                                                                             in_variable_fill_value))
                    except (AttributeError, TypeError):
                        in_variable_data = in_variable[...]
                except KeyError:
                    variable_presence = False
                    in_variable_data = None
                if variable_presence:
                    work_variable_datatypes[in_index][variable_name] = in_variable_data.dtype
                    work_variable_dimensions[in_index][variable_name] = in_variable.dimensions
                    try:
                        work_variable_fill_value[in_index][variable_name] = in_variable._FillValue
                    except AttributeError:
                        work_variable_fill_value[in_index][variable_name] = None
                    try:
                        time_dimension_condition = in_variable_data.shape[0] == time_dimensions[in_index]
                    except IndexError:
                        time_dimension_condition = False
                    if first_file:
                        variable_attributes[variable_name] = \
                            {attr: in_variable.getncattr(attr) for attr in in_variable.ncattrs()
                             if attr not in ['_FillValue', 'scale_factor', 'valid_min', 'valid_max']}

                else:
                    for first_available_index in range(len(time_sorted_list)):
                        first_available_file = time_sorted_list[first_available_index]
                        first_available_data = netCDF4.Dataset(first_available_file, mode='r')
                        try:
                            first_available_variable = first_available_data.variables[variable_name]
                            first_available_variable_data = first_available_variable[...]
                            break
                        except KeyError:
                            first_available_data.close()

                    if not first_available_variable_data.shape:
                        in_variable_data = np.ma.masked
                    elif first_available_variable_data.shape[0] == time_dimensions[first_available_index]:
                        if depth_dimension_name in first_available_variable.dimensions:
                            in_variable_data = np.ma.masked_all(shape=(int(time_dimensions[in_index]),
                                                                       out_depth_dimension)
                                                                + first_available_variable_data.shape[2:],
                                                                dtype=first_available_variable_data.dtype)
                        else:
                            in_variable_data = np.ma.masked_all(shape=(time_dimensions[in_index],)
                                                                + first_available_variable_data.shape[1:],
                                                                dtype=first_available_variable_data.dtype)
                    elif depth_dimension_name in first_available_variable.dimensions:
                        in_variable_data = np.ma.masked_all(shape=(out_depth_dimension,)
                                                            + first_available_variable_data.shape[1:],
                                                            dtype=first_available_variable_data.dtype)
                    else:
                        in_variable_data = np.ma.masked_all(shape=first_available_variable_data.shape,
                                                            dtype=first_available_variable_data.dtype)

                    work_variable_datatypes[in_index][variable_name] = None
                    work_variable_dimensions[in_index][variable_name] = None
                    work_variable_fill_value[in_index][variable_name] = None
                    time_dimension_condition = \
                        first_available_variable_data.shape[0] == time_dimensions[first_available_index]
                    if first_file:
                        variable_attributes[variable_name] = \
                            {attr: first_available_variable.getncattr(attr)
                             for attr in first_available_variable.ncattrs()
                             if attr not in ['_FillValue', 'scale_factor', 'valid_min', 'valid_max']}

                if variable_presence:
                    if time_dimension_condition:
                        if depth_dimension_name in in_variable.dimensions:
                            work_variable_data[in_index][variable_name] =\
                                np.ma.masked_all(shape=(int(time_dimensions[in_index]), out_depth_dimension)
                                                 + in_variable_data.shape[2:],
                                                 dtype=in_variable_data.dtype)
                            work_variable_data[in_index][variable_name].mask[:, : depth_dimensions[in_index], ...] = \
                                False
                            work_variable_data[in_index][variable_name][:, : depth_dimensions[in_index], ...] = \
                                np.ma.copy(in_variable_data)
                        elif not np.ma.is_masked(in_variable_data):
                            work_variable_data[in_index][variable_name] = \
                                np.ma.array(np.copy(in_variable_data),
                                            mask=np.zeros(shape=in_variable_data.shape, dtype=bool))
                        else:
                            work_variable_data[in_index][variable_name] = np.ma.copy(in_variable_data)
                    else:
                        if not in_variable_data.shape:
                            work_variable_data[in_index][variable_name] = in_variable_data
                        else:
                            if depth_dimension_name in in_variable.dimensions:
                                work_variable_data[in_index][variable_name] = \
                                    np.empty(shape=(out_depth_dimension,) + in_variable_data.shape[1:],
                                             dtype=in_variable_data.dtype)
                                work_variable_data[in_index][variable_name][: depth_dimensions[in_index], ...] = \
                                    np.copy(in_variable_data)
                            else:
                                work_variable_data[in_index][variable_name] = np.ma.copy(in_variable_data)
                else:
                    if time_dimension_condition:
                        if depth_dimension_name in first_available_variable.dimensions:
                            work_variable_data[in_index][variable_name] =\
                                np.ma.masked_all(shape=(int(time_dimensions[in_index]), out_depth_dimension)
                                                 + first_available_variable_data.shape[2:],
                                                 dtype=first_available_variable_data.dtype)
                        else:
                            work_variable_data[in_index][variable_name] = \
                                np.ma.masked_all(shape=(int(time_dimensions[in_index]),)
                                                 + first_available_variable_data.shape[1:],
                                                 dtype=first_available_variable_data.dtype)
                    else:
                        if not in_variable_data.shape:
                            work_variable_data[in_index][variable_name] = np.ma.masked
                        else:
                            if depth_dimension_name in first_available_variable_data.dimensions:
                                work_variable_data[in_index][variable_name] = \
                                    np.ma.masked_all(shape=(out_depth_dimension,)
                                                     + first_available_variable_data.shape[1:],
                                                     dtype=first_available_variable_data.dtype)
                            else:
                                work_variable_data[in_index][variable_name] = \
                                    np.ma.masked_all(shape=first_available_variable_data.shape,
                                                     dtype=first_available_variable_data.dtype)
                    first_available_data.close()

            first_file = False

            if verbose:
                print(' Closing dataset ' + in_file + '.')
            in_data.close()
        if verbose:
            print(' -------------------------')
        skip_indices = {}
        out_variable_data = {}
        out_variable_dimensions = {}
        out_variable_datatypes = {}
        out_variable_fill_value = {}
        for variable_name in out_variables:
            if verbose:
                print(' Initalizing concatenated variable ' + variable_name + '.')
            out_variable_dimensions[variable_name] = \
                max([work_variable_dimensions[index][variable_name]
                     for index in range(len(work_variable_dimensions))
                     if work_variable_dimensions[index][variable_name] is not None])
            data_types_intersection = intersect([work_variable_datatypes[index][variable_name].str
                                                for index in range(len(work_variable_datatypes))
                                                if work_variable_datatypes[index][variable_name] is not None])
            try:
                out_variable_datatypes[variable_name] = np.dtype(data_types_intersection)
            except TypeError:
                time.sleep(sleep_time)
                print(' Error: Type compatibility error for variable ' + variable_name + '.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                return

            try:
                fill_value_list = [work_variable_fill_value[index][variable_name]
                                   for index in range(len(work_variable_fill_value))
                                   if work_variable_fill_value[index][variable_name] is not None]
                try:
                    out_variable_fill_value[variable_name] = \
                        max([abs(fill_value) for fill_value in fill_value_list])
                    if out_variable_fill_value[variable_name] not in fill_value_list:
                        out_variable_fill_value[variable_name] = -out_variable_fill_value[variable_name]
                except TypeError:
                    out_variable_fill_value[variable_name] = \
                        next(fill_value for fill_value in fill_value_list if fill_value is not None)
            except ValueError:
                out_variable_fill_value[variable_name] = None
            if not out_variable_dimensions[variable_name]:
                out_variable_data[variable_name] = \
                    np.empty(shape=(0,), dtype=out_variable_datatypes[variable_name])
            elif work_variable_data[in_index][variable_name].shape[0] == time_dimensions[in_index]:
                if depth_dimension_name in out_variable_dimensions[variable_name]:
                    out_variable_data[variable_name] = \
                        np.ma.empty(shape=(0, out_depth_dimension)
                                    + work_variable_data[in_index][variable_name].shape[2:],
                                    fill_value=out_variable_fill_value[variable_name],
                                    dtype=out_variable_datatypes[variable_name])
                else:
                    out_variable_data[variable_name] = \
                        np.ma.empty(shape=(0,) + work_variable_data[in_index][variable_name].shape[1:],
                                    fill_value=out_variable_fill_value[variable_name],
                                    dtype=out_variable_datatypes[variable_name])
            else:
                if depth_dimension_name in out_variable_dimensions[variable_name]:
                    out_variable_data[variable_name] = \
                        np.empty(shape=(out_depth_dimension,)
                                 + work_variable_data[in_index][variable_name].shape[1:] + (0,),
                                 dtype=out_variable_datatypes[variable_name])
                else:
                    out_variable_data[variable_name] = \
                        np.empty(shape=work_variable_data[in_index][variable_name].shape + (0,),
                                 dtype=out_variable_datatypes[variable_name])
        if verbose:
            print(' -------------------------')
        couples_checked_list = list()
        for in_index in range(len(work_variable_data)):
            if verbose:
                print(' Masking duplicates for all selected variables in file ' +
                      os.path.basename(time_sorted_list[in_index]) + '...')
            if time_interceptions[in_index]:
                for second_index in time_interceptions[in_index]:
                    print('  With file ' + os.path.basename(time_sorted_list[second_index]) + '.')
                    couple_checking = [min(in_index, second_index), max(in_index, second_index)]
                    if couple_checking in couples_checked_list:
                        continue
                    couples_checked_list.append(couple_checking)
                    in_intersection_time_indices = \
                        np.array([index for index in range(len(work_time_seconds[in_index])) if
                                  work_time_seconds[in_index][index] in work_time_seconds[second_index]])
                    if in_intersection_time_indices.shape[0] == 0:
                        continue
                    second_intersection_time_indices = \
                        np.array([index for index in range(len(work_time_seconds[second_index])) if
                                  work_time_seconds[second_index][index] in work_time_seconds[in_index]])
                    if second_intersection_time_indices.shape[0] == 0:
                        continue
                    if second_intersection_time_indices.shape[0] != in_intersection_time_indices.shape[0]:
                        in_intersection_time_indices = \
                            np.array([index_2 for index in range(len(work_time_seconds[in_index]))
                                      if work_time_seconds[in_index][index] in work_time_seconds[second_index]
                                      for index_2 in [index] * np.sum(work_time_seconds[second_index] ==
                                                                      work_time_seconds[in_index][index])])

                        second_intersection_time_indices = \
                            np.array([index_2 for index in range(len(work_time_seconds[second_index]))
                                      if work_time_seconds[second_index][index] in work_time_seconds[in_index]
                                      for index_2 in [index] * np.sum(work_time_seconds[in_index] ==
                                                                      work_time_seconds[second_index][index])])
                    if (depth_variable_name != '') and \
                            (time_dimension_name in out_variable_dimensions[depth_variable_name]):
                        depth_intersection_condition = \
                            work_variable_data[in_index][depth_variable_name].mask[in_intersection_time_indices, ...] \
                            == work_variable_data[second_index][depth_variable_name].mask[
                                second_intersection_time_indices, ...]
                    if (time_steps[in_index] <= time_steps[second_index]) or \
                            (work_variable_datatypes[in_index] == np.float64 and
                             (work_variable_datatypes[second_index] == np.float32)) or \
                            (work_variable_datatypes[in_index] == np.complex128 and
                             (work_variable_datatypes[second_index] == np.complex64)) or \
                            (work_variable_datatypes[in_index] == np.uint64 and
                             ((work_variable_datatypes[second_index] == np.uint32) or
                              (work_variable_datatypes[second_index] == np.uint16) or
                              (work_variable_datatypes[second_index] == np.uint8))) or \
                            (work_variable_datatypes[in_index] == np.int64 and
                             ((work_variable_datatypes[second_index] == np.int32) or
                              (work_variable_datatypes[second_index] == np.int16) or
                              (work_variable_datatypes[second_index] == np.int8))) or \
                            (variables_number[in_index] >= variables_number[second_index]) or \
                            (depth_dimensions[in_index] >= depth_dimensions[second_index]):
                        for variable_name in out_variables:
                            try:
                                time_dimension_condition = \
                                    work_variable_data[in_index][variable_name].shape[0] == time_dimensions[in_index]
                            except IndexError:
                                time_dimension_condition = False
                            if time_dimension_condition:
                                time_mask_condition = np.invert(work_variable_data[in_index][variable_name].mask[
                                                                    in_intersection_time_indices, ...])
                                if depth_dimension_name in out_variable_dimensions[variable_name]:
                                    if depth_variable_name != '':
                                        if time_dimension_name in out_variable_dimensions[depth_variable_name]:
                                            mask_condition = np.logical_and(depth_intersection_condition,
                                                                            time_mask_condition)
                                            work_variable_data[second_index][variable_name].mask[
                                                second_intersection_time_indices, ...] = \
                                                np.where(mask_condition, mask_condition,
                                                         work_variable_data[second_index][variable_name].mask[
                                                             second_intersection_time_indices, ...])
                                        else:
                                            work_variable_data[second_index][variable_name].mask[
                                                second_intersection_time_indices, ...] = \
                                                np.where(time_mask_condition, time_mask_condition,
                                                         work_variable_data[second_index][variable_name].mask[
                                                             second_intersection_time_indices, ...])
                                    else:
                                        work_variable_data[second_index][variable_name].mask[
                                            second_intersection_time_indices, ...] = \
                                            np.where(time_mask_condition, time_mask_condition,
                                                     work_variable_data[second_index][variable_name].mask[
                                                         second_intersection_time_indices, ...])
                                elif depth_dimension_name in out_variable_dimensions[variable_name]:
                                    for second_depth_index in range(len(work_variable_data[second_index][
                                                                            depth_variable_name])):
                                        second_depth_value = work_variable_data[second_index][depth_variable_name][
                                            second_depth_index]
                                        if second_depth_value in work_variable_data[in_index][depth_variable_name]:
                                            in_depth_index = np.where(work_variable_data[in_index][depth_variable_name]
                                                                      == second_depth_value)[0][0]
                                            mask_condition = \
                                                np.invert(work_variable_data[in_index][variable_name].mask[
                                                              in_intersection_time_indices, in_depth_index, ...])
                                            work_variable_data[second_index][variable_name].mask[
                                                second_intersection_time_indices, second_depth_index, ...] = \
                                                np.where(mask_condition, mask_condition,
                                                         work_variable_data[second_index][variable_name].mask[
                                                             second_intersection_time_indices, second_depth_index, ...])
                                else:
                                    work_variable_data[second_index][variable_name].mask[
                                        second_intersection_time_indices, ...] = \
                                        np.where(time_mask_condition, time_mask_condition,
                                                 work_variable_data[second_index][variable_name].mask[
                                                     second_intersection_time_indices, ...])
                            else:
                                continue
                    elif (time_steps[in_index] > time_steps[second_index]) or \
                            (work_variable_datatypes[second_index] == np.float64 and
                             (work_variable_datatypes[in_index] == np.float32)) or \
                            (work_variable_datatypes[second_index] == np.complex128 and
                             (work_variable_datatypes[in_index] == np.complex64)) or \
                            (work_variable_datatypes[second_index] == np.uint64 and
                             ((work_variable_datatypes[in_index] == np.uint32) or
                              (work_variable_datatypes[in_index] == np.uint16) or
                              (work_variable_datatypes[in_index] == np.uint8))) or \
                            (work_variable_datatypes[second_index] == np.int64 and
                             ((work_variable_datatypes[in_index] == np.int32) or
                              (work_variable_datatypes[in_index] == np.int16) or
                              (work_variable_datatypes[in_index] == np.int8))) or \
                            (variables_number[in_index] < variables_number[second_index]) or \
                            (depth_dimensions[in_index] < depth_dimensions[second_index]):
                        for variable_name in out_variables:
                            try:
                                time_dimension_condition = \
                                    work_variable_data[second_index][variable_name].shape[0] == \
                                    time_dimensions[second_index]
                            except IndexError:
                                time_dimension_condition = False
                            if time_dimension_condition:
                                time_mask_condition = np.invert(work_variable_data[second_index][variable_name].mask[
                                                                    second_intersection_time_indices, ...])
                                if depth_dimension_name in out_variable_dimensions[variable_name]:
                                    if depth_variable_name != '':
                                        if depth_dimension_name in out_variable_dimensions[depth_variable_name]:
                                            mask_condition = np.logical_and(depth_intersection_condition,
                                                                            time_mask_condition)
                                            work_variable_data[second_index][variable_name].mask[
                                                in_intersection_time_indices, ...] = \
                                                np.where(mask_condition, mask_condition,
                                                         work_variable_data[in_index][variable_name].mask[
                                                             in_intersection_time_indices, ...])
                                        else:
                                            work_variable_data[in_index][variable_name].mask[
                                                in_intersection_time_indices, ...] = \
                                                np.where(time_mask_condition, time_mask_condition,
                                                         work_variable_data[in_index][variable_name].mask[
                                                             in_intersection_time_indices, ...])
                                    else:
                                        work_variable_data[in_index][variable_name].mask[
                                            in_intersection_time_indices, ...] = \
                                            np.where(time_mask_condition, time_mask_condition,
                                                     work_variable_data[in_index][variable_name].mask[
                                                         in_intersection_time_indices, ...])
                                elif depth_dimension_name in out_variable_dimensions[variable_name]:
                                    for in_depth_index in range(len(work_variable_data[in_index][
                                                                            depth_variable_name])):
                                        in_depth_value = work_variable_data[in_index][depth_variable_name][
                                            in_depth_index]
                                        if in_depth_value in work_variable_data[in_index][depth_variable_name]:
                                            second_depth_index = \
                                                np.where(work_variable_data[second_index][depth_variable_name] ==
                                                         in_depth_value)[0][0]
                                            mask_condition = \
                                                np.invert(work_variable_data[second_index][variable_name].mask[
                                                              second_intersection_time_indices,
                                                              second_depth_index, ...])
                                            work_variable_data[in_index][variable_name].mask[
                                                in_intersection_time_indices, in_depth_index, ...] = \
                                                np.where(mask_condition, mask_condition,
                                                         work_variable_data[in_index][variable_name].mask[
                                                             in_intersection_time_indices, in_depth_index, ...])
                                else:
                                    work_variable_data[in_index][variable_name].mask[
                                        in_intersection_time_indices, ...] = \
                                        np.where(time_mask_condition, time_mask_condition,
                                                 work_variable_data[in_index][variable_name].mask[
                                                     in_intersection_time_indices, ...])
                            else:
                                continue
                    else:
                        continue
        if verbose:
            print(' -------------------------')
        for in_index in range(len(work_variable_data)):
            if verbose:
                print(' Checking if file ' +
                      os.path.basename(time_sorted_list[in_index]) + ' can then be skipped...')
            skip_indices[in_index] = True
            for variable_name in out_variables:
                try:
                    time_dimension_condition = \
                        work_variable_data[in_index][variable_name].shape[0] == time_dimensions[in_index]
                except IndexError:
                    time_dimension_condition = False
                if time_dimension_condition:
                    if not np.all(work_variable_data[in_index][variable_name].mask):
                        skip_indices[in_index] = False
                    break
                else:
                    continue
            if skip_indices[in_index] and verbose:
                print(' Yes.')
            elif verbose:
                print(' No.')
        if verbose:
            print(' -------------------------')
        concatenated_time = False
        for variable_name in out_variables:
            if verbose:
                print(' Concatenating variable ' + variable_name + '.')
            for in_index in range(len(work_variable_data)):
                if not skip_indices[in_index]:
                    try:
                        time_dimension_condition = \
                            work_variable_data[in_index][variable_name].shape[0] == time_dimensions[in_index]
                    except IndexError:
                        time_dimension_condition = False
                    if time_dimension_condition:
                        out_variable_data[variable_name] = np.ma.append(out_variable_data[variable_name],
                                                                        work_variable_data[in_index][variable_name],
                                                                        axis=0)
                    elif not in_variable_data.shape:
                        out_variable_data[variable_name] = np.append(out_variable_data[variable_name],
                                                                     work_variable_data[in_index][variable_name])
                    else:
                        out_variable_data[variable_name] = \
                            np.append(out_variable_data[variable_name],
                                      work_variable_data[in_index][variable_name][..., np.newaxis], axis=-1)
                    if not concatenated_time:
                        out_time_data = np.ma.append(out_time_data, work_time_data[in_index], axis=0)
            concatenated_time = True

        last_time_dimension_size = output_dimensions_sizes[output_dimensions.index(time_dimension_name)]
        for dimension_number in range(len(output_dimensions)):
            dimension_name = output_dimensions[dimension_number]
            dimension_size = output_dimensions_sizes[dimension_number]
            if (dimension_size == last_time_dimension_size) and (dimension_name not in ['lon', 'lat', 'depth']):
                output_dimensions_sizes[dimension_number] = out_time_data.shape[0]
            elif dimension_name == depth_dimension_name:
                output_dimensions_sizes[dimension_number] = out_depth_dimension

        if verbose:
            print(' Creating output dataset ' + out_file + '.')
        out_data = netCDF4.Dataset(out_file, mode='w', format='NETCDF4')
        for dimension_number in range(len(output_dimensions)):
            dimension_name = output_dimensions[dimension_number]
            dimension_size = output_dimensions_sizes[dimension_number]
            if dimension_name == time_dimension_name:
                out_data.createDimension(time_dimension_name, None)
            else:
                out_data.createDimension(dimension_name, dimension_size)

        if verbose:
            print(' Creating concatenated variable ' + time_variable_name + '.')
        out_time = out_data.createVariable(time_variable_name,
                                           datatype=out_time_datatype,
                                           dimensions=(time_dimension_name,),
                                           fill_value=time_fill_value, zlib=True, complevel=1)
        out_time[...] = out_time_data
        out_time.setncatts(time_attributes)

        out_variable = {}
        for variable_name in out_variables:
            if verbose:
                print(' Writing concatenated variable ' + variable_name + '.')

            if out_variable_data[variable_name].shape[0] == out_time_data.shape[0]:
                out_variable[variable_name] = out_data.createVariable(variable_name,
                                                                      datatype=out_variable_datatypes[variable_name],
                                                                      dimensions=out_variable_dimensions[variable_name],
                                                                      fill_value=out_variable_fill_value[variable_name],
                                                                      zlib=True, complevel=1)
                out_variable[variable_name][...] = out_variable_data[variable_name]
            else:
                out_variable[variable_name] = out_data.createVariable(variable_name,
                                                                      datatype=out_variable_datatypes[variable_name],
                                                                      dimensions=out_variable_dimensions[variable_name],
                                                                      fill_value=out_variable_fill_value[variable_name])
                try:
                    out_variable[variable_name][...] = np.ma.mean(out_variable_data[variable_name], axis=-1)
                except TypeError:
                    out_variable[variable_name][...] = out_variable_data[variable_name][..., -1]

            out_variable[variable_name].setncatts(variable_attributes[variable_name])

        if verbose:
            print(' Setting global attributes.')
        # Set global attributes
        global_attributes_names = global_attributes.keys()
        global_attributes_names = [element for element in global_attributes_names if not element.startswith('id')]
        global_attributes_names = [element for element in global_attributes_names if not element.startswith('history')]
        global_attributes_names = [element for element in global_attributes_names if not element.startswith('date')]
        global_attributes_names = [element for element in global_attributes_names if not element.startswith('time')]
        global_attributes_names = [element for element in global_attributes_names if not element.startswith('last')]
        out_data.setncatts({attr: global_attributes[attr] for attr in global_attributes_names})
        out_data.history = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + ' : Concatenation created'
        out_data.date_update = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        if 'id' in global_attributes.keys():
            out_data.id = os.path.basename(out_file).split('.')[0]
        if 'geospatial_lon_min' in global_attributes.keys():
            out_data.geospatial_lon_min = str(np.ma.min(out_variable_data[longitude_variable_name][...]))
        if 'geospatial_lon_max' in global_attributes.keys():
            out_data.geospatial_lon_max = str(np.ma.max(out_variable_data[longitude_variable_name][...]))
        if 'geospatial_lat_min' in global_attributes.keys():
            out_data.geospatial_lat_min = str(np.ma.min(out_variable_data[latitude_variable_name][...]))
        if 'geospatial_lat_max' in global_attributes.keys():
            out_data.geospatial_lat_max = str(np.ma.max(out_variable_data[latitude_variable_name][...]))
        if 'geospatial_vertical_min' in global_attributes.keys():
            try:
                out_data.geospatial_vertical_min = str(np.ma.min(out_variable_data[depth_variable_name][...]))
            except KeyError:
                out_data.geospatial_vertical_min = '0'
        if 'geospatial_vertical_max' in global_attributes.keys():
            try:
                out_data.geospatial_vertical_max = str(np.ma.max(out_variable_data[depth_variable_name][...]))
            except KeyError:
                out_data.geospatial_vertical_max = '0'
        if 'time_coverage_start' in global_attributes.keys():
            out_data.time_coverage_start = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time_starts[0]))
        if 'time_coverage_end' in global_attributes.keys():
            out_data.time_coverage_end = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time_ends[-1]))
        if 'last_date_observation' in global_attributes.keys():
            out_data.last_date_observation = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time_ends[-1]))

        if verbose:
            print(' Closing output dataset ' + out_file + '.')
            print(' -------------------------')
        out_data.close()


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_list = sys.argv[1]
        out_file = sys.argv[2]

    except (IndexError, ValueError):
        in_list = None
        out_file = None

    try:
        in_fields_standard_name_str = sys.argv[3]
    except (IndexError, ValueError):
        in_fields_standard_name_str = None

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

    pointwise_datasets_concatenator(in_list, out_file, in_fields_standard_name_str,
                                    first_date_str, last_date_str, verbose)
