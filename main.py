from modules.extract.extract_utils import make_get_request, generate_water_level_url
from modules.logger.logger import create_logger
from modules.load.load import connect_to_mysql, load_to_database
from modules.transform.transform import transform_station_info, parse_json_file, transform_water_level
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
        STATION_INFO_FORMAT = stations[station_name]["STATION_INFO_FORMAT"]
        PRODUCT_FORMAT = stations[station_name]["PRODUCT_FORMAT"]
        STATION_ID = stations[station_name]["ID"]
        WATER_LEVEL_URL = generate_water_level_url(station_id=STATION_ID, 
                                   datum=stations[station_name]["DATUM"],
                                   data_format=PRODUCT_FORMAT,
                                   date="latest")
        STATION_INFO_URL = f"https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{STATION_ID}.{STATION_INFO_FORMAT}?expand=details,products&units=english"

        station_info = make_get_request(STATION_INFO_URL, HEADERS, extract_logger, STATION_INFO_FORMAT)
        station_info = transform_station_info(station_info, STATION_INFO_FORMAT)

        product = make_get_request(WATER_LEVEL_URL, HEADERS, extract_logger, PRODUCT_FORMAT)
        product = transform_water_level(product, PRODUCT_FORMAT, STATION_ID)

        load_to_database(cnx=cnx, logger=load_logger, data=station_info, load_query=DB_QUERIES['station_info_load_query'])

        for arr in product:
            load_to_database(cnx=cnx, logger=load_logger, data=arr, load_query=DB_QUERIES['water_level_load_query'])

        load_logger.info(f"Loaded data into database for station {station_info[1]} identified by ID: {station_info[0]}")

except Exception as e:
    extract_logger.critical(f"An unexpected error occurred: {e}")
    print(e)

finally:
    if cnx and cnx.is_connected():
        cnx.close()


