from modules.extract.extract_utils import make_get_request, parse_json_file
from modules.logger.logger import create_logger
from modules.load.load import connect_to_mysql, load_to_database
from modules.transform.transform import transform_water_level_row
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DATABASE = os.getenv("DB_DATABASE")

current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, "config", "config.json")
log_files_path = os.path.join(current_dir, "logs")

config = parse_json_file(config_path)

HEADERS = config["API_HEADERS"]
API_CONFIG = config["API"]
DB_QUERIES = config["DB_QUERIES"]

stations = API_CONFIG["stations"]
station_info_response_columns = API_CONFIG["station_info_response_columns"]
water_level_response_columns = API_CONFIG["water_level_response_columns"]


load_logger = create_logger("load", log_files_path)
extract_logger = create_logger("extract", log_files_path)

cnx = connect_to_mysql({"host": DB_HOST, "user": DB_USER, 
                        "password": DB_PASSWORD, "database": DATABASE}, load_logger)


try:
    for station_name in stations:
        station_info = []

        DATUM = stations[station_name]["DATUM"]
        ID = stations[station_name]["ID"]
        API_URL = f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?date=today&station={ID}&product=water_level&datum={DATUM}&time_zone=gmt&units=english&format=json"
        
        response = make_get_request(API_URL, HEADERS, extract_logger)

        for col in station_info_response_columns:
            station_info.append(response["metadata"][col])
        
        load_to_database(cnx=cnx, logger=load_logger, data=station_info, load_query=DB_QUERIES['station_info_load_query'])

        for dct in response['data']:
            arr = [station_info[0]]
            transform_water_level_row(arr, dct, water_level_response_columns)
            load_to_database(cnx=cnx, logger=load_logger, data=arr, load_query=DB_QUERIES['water_level_load_query'])

        load_logger.info(f"Loaded data into database for station {station_info[1]} identified by ID: {station_info[0]}")

except Exception as e:
    extract_logger.critical(f"An unexpected error occurred: {e}")

finally:
    if cnx and cnx.is_connected():
        cnx.close()


