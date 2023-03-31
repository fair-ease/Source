# -*- coding: utf-8 -*-
from pykml.factory import nsmap
from pykml.factory import KML_ElementMaker as KML
from pykml.factory import GX_ElementMaker as GX
from lxml import etree
import sys
import os
import shlex
import numpy as np
import pandas as pd
import time
import calendar

# Global variables
sleep_time = 0.1  # seconds
yellow_bullet = 'https://imagizer.imageshack.com/v2/xq90/923/zUnsxd.png'

# Insitu fields
insitu_standard_names = ['sea_water_temperature', 'sea_water_practical_salinity',
                         'sea_surface_height_above_sea_level', 'eastward_sea_water_velocity',
                         'northward_sea_water_velocity', 'sea_water_speed', 'direction_of_sea_water_velocity',
                         'upward_sea_water_velocity']
field_long_names = ['Sea Water Temperature', 'Sea Water Practical Salinity',
                    'Sea Surface Height', 'Sea Water Zonal Current', 'Sea Water Meridional Current',
                    'Sea Water Speed', 'Sea Water Speed Direction', 'Sea Water Vertical Current']


# Function to calculate distance from two points in spherical coordinates
def earth_distance(lat1, lon1, lat2, lon2):
    earth_radius = 6.371e3  # Earth radius (km)
    deg_to_rad = np.pi / 180.0  # Sexagesimal - radiants conversion factor
    # sexagesimal - radiants conversion
    rdlat1 = lat1 * deg_to_rad
    rdlon1 = lon1 * deg_to_rad
    rdlat2 = lat2 * deg_to_rad
    rdlon2 = lon2 * deg_to_rad
    # Distance calculus.
    # given A(lat1, lon1) e  B(lat2, lon2) on the unit sphere we have:
    # d(A, B) = arccos( cos(lon1 - lon2) * cos(lat1) * cos(lat2) + sin(lat1) * sin(lat2) )
    earth_dist = earth_radius * np.arccos(np.cos(rdlon1 - rdlon2) * np.cos(rdlat1) * np.cos(rdlat2)
                                          + np.sin(rdlat1) * np.sin(rdlat2))
    return earth_dist


# Load input arguments
try:
    in_csv_dir = sys.argv[1]
    in_fields_standard_name_str = sys.argv[2]
    out_kml_file = sys.argv[3]
except (IndexError, ValueError):
    in_csv_dir = None
    in_fields_standard_name_str = None
    out_kml_file = None

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
    region_boundaries_str = sys.argv[6]
except (IndexError, ValueError):
    region_boundaries_str = '-180 180 0 180'


# Functional version
def create_probes_earth_map(in_csv_dir=None, in_fields_standard_name_str=None, out_kml_file=None,
                            first_date_str=None, last_date_str=None, region_boundaries_str='-180 180 0 180'):
    """
    Script to create Google Earth KML file with mooring locations imported from CSV file.

    Input arguments:

        1) In situ information CSV directory: Directory with (almost) the sequent files):
            a) Devices information CSV file, with the sequent header:
                Device ID, Name;
            b) Organizations information CSV file, with the sequent header:
                Organization ID, Name, Country, Link;
            c) Variables information CSV file, with the sequent header:
                Variable ID, long_name, standard_name, units;
            d) Probes information CSV file, with the sequent header:
                Probe ID, platform_code, name, WMO, device type ID, organization ID, variable IDs,
                average longitudes, average latitudes, record starts, record ends, sampling times (ddd hh:mm:ss form),
                depth levels, quality controls, ancillary notes, weblink;

        2) Input variables standard_name attributes to process space separated string
            (for example: "sea_water_temperature sea_water_practical_salinity", please read CF conventions standard name
            table to find the correct strings to insert);

        3) Output KML file;

        4) Start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format (optional);

        5) End date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS (optional);

        6) Region longitude - latitude limits space separated string
            (min_lon, max_lon (deg E); min_lat, max_lat (deg N), OPTIONAL);

    Output is a Google Earth KML file, containing georeferenced map of probes with information.

    Written Oct 25, 2017 by Paolo Oliveri
    """
    # if __name__ == '__main__':
    #     return
    start_run_time = calendar.timegm(time.gmtime())
    print('-------------------------' + ' ' + __file__ + ' -------------------------')
    print(' Local time is ' + time.strftime("%a %b %d %H:%M:%S %Z %Y", time.gmtime()))
    print(' -------------------------')
    print(' Script to create Google Earth KML file with mooring locations imported from CSV file.')
    print(' -------------------------')
    if in_csv_dir is None or in_fields_standard_name_str is None or out_kml_file is None:
        time.sleep(sleep_time)
        print(' ERROR: 3 of 6 maximum arguments (3 optionals) not provided.', file=sys.stderr)
        print(' 1) In situ information CSV directory;', file=sys.stderr)
        print(' 2) Input fields standard_name space separated string to process'
              ' (for example: "sea_water_temperature sea_water_practical_salinity");', file=sys.stderr)
        print(' 3) Output KML file path and name;', file=sys.stderr)
        print(' 4) (optional) First date to evaluate in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: first recorded date for each device);', file=sys.stderr)
        print(' 5) (optional) Last date to evaluate in YYYYMMDD or YYYY-MM-DD HH:MM:SS format'
              ' (default: last recorded date for each device);', file=sys.stderr)
        print(' 6) (optional) Region longitude - latitude limits space separated string'
              ' (min_lon, max_lon (deg E); min_lat, max_lat (deg N), default: "-180 180 -90 90")',
              file=sys.stderr)
        time.sleep(sleep_time)
        return

    try:
        time.strptime(first_date_str, '%Y%m%d')
    except (IndexError, TypeError, ValueError):
        try:
            time.strptime(first_date_str, '%Y-%m-%d %H:%M:%S')
        except (IndexError, TypeError, ValueError):
            first_date_str = None

    try:
        time.strptime(last_date_str, '%Y%m%d')
    except (IndexError, TypeError, ValueError):
        try:
            time.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')
        except (IndexError, TypeError, ValueError):
            last_date_str = None

    if (region_boundaries_str is None) or (region_boundaries_str == 'None') or (region_boundaries_str == ''):
        region_boundaries_str = '-180 180 -90 90'

    print(' in situ information CSV directory = ' + in_csv_dir)
    print(' Input variables to process standard_name string = ' + in_fields_standard_name_str)
    print(' Output KML file = ' + out_kml_file)
    print(' First date to process = ' + str(first_date_str) +
          ' (if None it will be the first available date on each device)')
    print(' Last date to process = ' + str(last_date_str) +
          ' (if None it will be the last available date on each device)')
    print(' Region boundary horizontal limits (min_lon, max_lon (deg E); min_lat, max_lat (deg N)) = '
          + region_boundaries_str)
    print(' -------------------------')

    if (in_fields_standard_name_str is None) or (in_fields_standard_name_str == 'None') or \
            (in_fields_standard_name_str == '') or len(in_fields_standard_name_str.split(' ')) < 1:
        time.sleep(sleep_time)
        print(' Error. Wrong input fields string.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    else:
        in_fields_standard_name_list = shlex.split(in_fields_standard_name_str)

    # Box boundaries to process probes
    west_boundary = np.float32(region_boundaries_str.split(' ')[0])
    east_boundary = np.float32(region_boundaries_str.split(' ')[1])
    south_boundary = np.float32(region_boundaries_str.split(' ')[2])
    north_boundary = np.float32(region_boundaries_str.split(' ')[3])

    # define a variable for the Google Extensions namespace URL string
    gx_variable = '{' + nsmap['gx'] + '}'
    # Google Earth view centre, angle and tilt
    view_longitude = ((east_boundary + 180) + (west_boundary + 180)) / 2 - 180
    view_latitude = ((north_boundary + 90) + (south_boundary + 90)) / 2 - 90
    view_altitude = 0
    view_heading = 0.9
    view_tilt = 0
    view_range = (np.max([earth_distance(view_latitude, west_boundary, view_latitude, east_boundary),
                         earth_distance(south_boundary, view_longitude, north_boundary, view_longitude)])) * 1000

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

    print(' Loading devices information CSV file...')
    try:
        in_devices_data = open(in_csv_dir + '/devices.csv', 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty devices CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    in_devices_data =\
        pd.read_csv(in_devices_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if in_devices_data.ndim == 1:
        in_devices_data = in_devices_data[np.newaxis, :]

    device_ids = in_devices_data[:, 0]
    device_types = in_devices_data[:, 1]

    print(' Loading organizations information CSV file...')
    try:
        in_organizations_data = open(in_csv_dir + '/organizations.csv', 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty organizations CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    in_organizations_data =\
        pd.read_csv(in_organizations_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if in_organizations_data.ndim == 1:
        in_organizations_data = in_organizations_data[np.newaxis, :]

    organization_ids = in_organizations_data[:, 0]
    organization_names = in_organizations_data[:, 1]

    print(' Loading variables information CSV file...')
    try:
        in_variables_data = open(in_csv_dir + '/variables.csv', 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty variables CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    in_variables_data =\
        pd.read_csv(in_variables_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if in_variables_data.ndim == 1:
        in_variables_data = in_variables_data[np.newaxis, :]

    variable_ids = in_variables_data[:, 0]
    variable_standard_names = in_variables_data[:, 1]

    print(' Loading probes information CSV file...')
    try:
        in_probes_data = open(in_csv_dir + '/probes.csv', 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty probes CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    in_probes_data =\
        pd.read_csv(in_probes_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if in_probes_data.ndim == 1:
        in_probes_data = in_probes_data[np.newaxis, :]

    probes_platform_codes = in_probes_data[:, 1]
    probes_names = in_probes_data[:, 2]
    probes_wmo = in_probes_data[:, 3]
    probes_device_type_ids = in_probes_data[:, 4]
    probes_types = np.copy(probes_device_type_ids)
    for index in range(len(device_types)):
        probes_types = np.where(probes_types == device_ids[index], device_types[index], probes_types)
    probes_organization_ids = in_probes_data[:, 5]
    probes_organizations = np.copy(probes_organization_ids)
    for index in range(len(organization_names)):
        probes_organizations =\
            np.where(probes_organizations == organization_ids[index], organization_names[index], probes_organizations)
    probes_variables_ids = [standard_names.split(';') for standard_names in in_probes_data[:, 6]]
    probes_standard_names = probes_variables_ids
    for index in range(len(probes_variables_ids)):
        for index_id in range(len(variable_ids)):
            try:
                probe_variable_index = probes_standard_names[index].index(variable_ids[index_id])
                probes_standard_names[index][probe_variable_index] = variable_standard_names[index_id]
            except ValueError:
                continue
    probes_longitudes = [longitudes.split(';') for longitudes in in_probes_data[:, 7]]
    probes_latitudes = [latitudes.split(';') for latitudes in in_probes_data[:, 8]]
    probes_record_starts = [record_starts.split(';') for record_starts in in_probes_data[:, 9]]
    probes_record_ends = [record_ends.split(';') for record_ends in in_probes_data[:, 10]]
    probes_sampling_times = [sampling_times.split(';') for sampling_times in in_probes_data[:, 11]]
    probes_depths = \
        [[depths.split(' ') for depths in split_depths.split(';')] for split_depths in in_probes_data[:, 12]]
    probes_quality_controls = [quality_controls.split(';') for quality_controls in in_probes_data[:, 13]]
    probes_notes = [notes.split(';') for notes in in_probes_data[:, 14]]
    probes_links = in_probes_data[:, 15]

    out_probes_descriptions = list()  # np.empty(shape=in_probes_data.shape[0], dtype=object)
    out_probes_platform_codes = list()
    out_probes_names = list()
    out_probes_types = list()
    out_probes_longitudes = list()
    out_probes_latitudes = list()
    for insitu_device in range(in_probes_data.shape[0]):
        device_platform_code = probes_platform_codes[insitu_device]
        out_standard_names = [standard_name for standard_name in probes_standard_names[insitu_device]
                              if standard_name in in_fields_standard_name_list]
        if len(out_standard_names) == 0:
            continue
        device_fields_descriptions = list()
        for standard_name in out_standard_names:
            try:
                title_index = insitu_standard_names.index(standard_name)
            except ValueError:
                continue
            field_title = field_long_names[title_index]
            try:
                field_index = probes_standard_names[insitu_device].index(standard_name)
            except ValueError:
                continue
            if first_date_str is not None:
                if probes_record_starts[insitu_device][field_index] < first_date:
                    continue
            if last_date_str is not None:
                if probes_record_starts[insitu_device][field_index] > last_date:
                    continue
            if device_platform_code not in out_probes_platform_codes:
                out_probes_platform_codes.append(device_platform_code)
                out_probes_names.append(probes_names[insitu_device])
                out_probes_types.append(probes_types[insitu_device])
                out_probes_longitudes.append(probes_longitudes[insitu_device])
                out_probes_latitudes.append(probes_latitudes[insitu_device])

            device_fields_descriptions.append(
                'Field ' + standard_name +
                ' (' + field_title + '): from ' +
                probes_record_starts[insitu_device][field_index] + ' to ' +
                probes_record_ends[insitu_device][field_index] +
                ', average longitude ' + ''.join(probes_longitudes[insitu_device][field_index]) +
                ', average latitude ' + ''.join(probes_latitudes[insitu_device][field_index]) +
                ', sampling time ' + ''.join(probes_sampling_times[insitu_device][field_index]) +
                ', hh:mm:ss, depth levels ' + ', '.join(probes_depths[insitu_device][field_index]) + ' m'
                ', quality controls ' + ''.join(probes_quality_controls[insitu_device][field_index]) +
                ', notes ' + ''.join(probes_notes[insitu_device][field_index]) + '.')
        out_probes_descriptions.append(
            'Device platform code: ' + probes_platform_codes[insitu_device] + '\n\n'
            'Device type: ' + probes_types[insitu_device] + '\n\n'
            'Platform name and WMO (if present): ' + probes_names[insitu_device] +
            ' (' + probes_wmo[insitu_device] + ')\n\n'
            'Organisation: ' + probes_organizations[insitu_device] + '\n\n'
            'Link: ' + probes_links[insitu_device] + '\n\n'
            'RECORDED DATA INFORMATION\n\n' + '\n\n'.join(device_fields_descriptions))

    print(' Creating KML skeleton...')

    kml_document = KML.kml(
        KML.Style(KML.ListStyle(KML.ListItemType('check'),
                                KML.ItemIcon(KML.state('open'),
                                             KML.href(':/mysavedplaces_open.png'),
                                             ),
                                KML.ItemIcon(KML.state('closed'),
                                             KML.href(':/mysavedplaces_closed.png'),
                                             ),
                                ),
                  KML.ListStyle(KML.bgColor('00ffffff'),
                                KML.maxSnippetLines('2'),
                                ),
                  ),
        KML.Document(
            GX.Tour(
                KML.name('Mooring probes locations'),
                GX.Playlist(),
            ),
        )
    )

    for device_type_title in np.unique(probes_types):
        if any(device_type == device_type_title for device_type in out_probes_types):
            device_folder = KML.Folder(
                KML.name(device_type_title),
                id=device_type_title,
            )

            for insitu_device in range(len(out_probes_platform_codes)):
                # device_name = out_probes_names[insitu_device]
                device_longitudes = out_probes_longitudes[insitu_device]
                device_latitudes = out_probes_latitudes[insitu_device]
                device_longitude = str(np.mean(np.array(device_longitudes, dtype=float)))
                device_latitude = str(np.mean(np.array(device_latitudes, dtype=float)))

                if out_probes_types[insitu_device] == device_type_title:
                    device_folder.append(
                        KML.Placemark(  # KML.name(out_probes_names[insitu_device]),
                                      KML.description(out_probes_descriptions[insitu_device]),
                                      KML.Style(KML.IconStyle(KML.scale('1.1'),
                                                              KML.Icon(KML.href(yellow_bullet)),
                                                              ),
                                                KML.LabelStyle(KML.scale('1.5')),
                                                ),
                                      KML.Point(KML.coordinates(device_longitude + ', '
                                                                + device_latitude)),
                                      ),
                    )

            kml_document.Document.append(device_folder)

    kml_document.Document[gx_variable + 'Tour'].Playlist.append(
        GX.FlyTo(
            GX.flyToMode('smooth'),
            KML.LookAt(KML.latitude(str(view_latitude)), KML.longitude(str(view_longitude)),
                       KML.altitude(str(view_altitude)), KML.heading(str(view_heading)),
                       KML.tilt(str(view_tilt)), KML.range(str(view_range)),
                       GX.altitudeMode('relativeToGround')
                       ),
        ),
    )

    print(' Writing KML document to output file...')

    kml_file = open(out_kml_file, mode='wb')

    kml_file.write(etree.tostring(kml_document, pretty_print=True))

    kml_file.close()

    total_run_time = time.gmtime(calendar.timegm(time.gmtime()) - start_run_time)
    print(' Finished! Total elapsed time is: ' + time.strftime('%H:%M:%S', total_run_time) + ' hh:mm:ss')


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_csv_dir = sys.argv[1]
        in_fields_standard_name_str = sys.argv[2]
        out_kml_file = sys.argv[3]
    except (IndexError, ValueError):
        in_csv_dir = None
        in_fields_standard_name_str = None
        out_kml_file = None

    try:
        first_date_str = sys.argv[4]
    except (IndexError, ValueError):
        first_date_str = None

    try:
        last_date_str = sys.argv[5]
    except (IndexError, ValueError):
        last_date_str = None

    try:
        region_boundaries_str = sys.argv[6]
    except (IndexError, ValueError):
        region_boundaries_str = '-180 180 0 180'

    create_probes_earth_map(in_csv_dir, in_fields_standard_name_str, out_kml_file,
                            first_date_str, last_date_str, region_boundaries_str)
