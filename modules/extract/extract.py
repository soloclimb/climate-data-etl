import os
from ..logger.logger import create_logger
from ..load.load import connect_to_mysql, load_to_database
from .extract_utils import make_get_request, parse_json_file

current_dir = os.path.dirname(os.path.abspath(__file__))

def api_request():

    db_config_path = os.path.join(current_dir, '../../', "config", "db_config.json")
    headers_path = os.path.join(current_dir, '../../', "config", "api_request_headers.json")
    extract_config_path = os.path.join(current_dir, '../../', "config", "extract_config.json")
    log_files_path = os.path.join(current_dir, '../../', "logs")

    HEADERS = parse_json_file(headers_path)
    DB_CONFIG = parse_json_file(db_config_path)

    load_logger = create_logger("load", log_files_path)
    extract_logger = create_logger("extract", log_files_path)

    cnx = connect_to_mysql(DB_CONFIG, load_logger)
    try:
        items = parse_json_file(extract_config_path)["API"]
        station_info_response_columns = items["station_info_response_columns"]
        water_level_response_columns = items["water_level_response_columns"]
        
        for station_name in items["stations"]:
            station_info = []
            
            DATUM = items["stations"][station_name]["DATUM"]
            ID = items["stations"][station_name]["ID"]
            API_URL = f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?date=today&station={ID}&product=water_level&datum={DATUM}&time_zone=gmt&units=english&format=json"
            
            response = make_get_request(API_URL, HEADERS, extract_logger)

            for col in station_info_response_columns:
                station_info.append(response["metadata"][col])

            station_info_load_query = "INSERT IGNORE INTO station_info (station_id, station_name, station_LAT, station_LON) VALUES (%s, %s, %s, %s)"
            
            load_to_database(cnx=cnx, logger=load_logger, data=station_info, load_query=station_info_load_query)

            for dct in response['data']:
                arr = [station_info[0]]
                for col in water_level_response_columns:
                    if col[0] == 'f':
                        arr.append(dct['f'].split(",")[int(col[2])])
                    else:
                        arr.append(dct[col])

                water_level_load_query = "INSERT IGNORE INTO water_level (station_id, record_time, water_level, sigma, water_level_inferred, flat_tolerance_exceeded, expected_water_level_exceeded) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                load_to_database(cnx=cnx, logger=load_logger, data=arr, load_query=water_level_load_query)

            load_logger.info(f"Loaded data into database for station {station_info[1]} identified by ID: {station_info[0]}")
    except Exception as e:
        extract_logger.critical(f"An unexpected error occurred: {e}")

    finally:
        if cnx and cnx.is_connected():
            cnx.close()