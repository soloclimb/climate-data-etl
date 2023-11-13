import os
import requests
import json
from ..logger.logger import create_logger
from ..load.load_data import connect_to_mysql, load_to_database

current_dir = os.path.dirname(os.path.abspath(__file__))

def api_request():
    db_config_path = os.path.join(current_dir, '../../', "config", "db_config.json")
    headers_path = os.path.join(current_dir, '../../', "config", "api_request_headers.json")
    extract_config_path = os.path.join(current_dir, '../../', "config", "extract_config.json")
    log_files_path = os.path.join(current_dir, '../../', "logs")

    db_config = {}
    HEADERS = {}


    with open(headers_path, "r") as headers_file:
        HEADERS = json.load(headers_file)

    with open(db_config_path, "r") as config_file:
      try:
        db_config = json.load(config_file)
      except json.decoder.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"File content: {config_file.read()}")
        raise  # Re-raise the exception to terminate the script


    load_logger = create_logger("load", log_files_path)
    cnx = connect_to_mysql(db_config, load_logger)

    with open(extract_config_path, "r") as extract_config:
        items = json.load(extract_config)["API"]
        station_info_response_columns = items["station_info_response_columns"]
        water_level_response_columns = items["water_level_response_columns"]
        for station_name in items["stations"]:
            station_info = []
            DATUM = items["stations"][station_name]["DATUM"]
            ID = items["stations"][station_name]["ID"]
            API_URL = f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?date=today&station={ID}&product=water_level&datum={DATUM}&time_zone=gmt&units=english&format=json"
            response = requests.get(API_URL, headers=HEADERS).json()
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

        cnx.close()