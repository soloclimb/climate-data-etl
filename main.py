from scripts.extract.extract import make_get_request, generate_station_info_url , generate_product_url
from scripts.logger.logger import create_logger
from scripts.load.load import connect_to_mysql, load_to_database
from scripts.transform.transform import transform_station_info, transform_water_level, transform_water_temperature
from dotenv import load_dotenv
import os
from scripts.utils.parse_config import parse_product_config

load_dotenv()


current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, "config", "config.json")
log_files_path = os.path.join(current_dir, "logs")

wl_config = parse_product_config(config_path, "wl")
wt_config = parse_product_config(config_path, "wt")

load_logger = create_logger("load", log_files_path)
extract_logger = create_logger("extract", log_files_path)

cnx = connect_to_mysql({"host": DB_HOST, "user": DB_USER, 
                        "password": DB_PASSWORD, "database": DATABASE}, load_logger)
def etl():
        

def etl(product_type, product_config):
    load_dotenv()

    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DATABASE = os.getenv("DB_DATABASE")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "config", "config.json")
    log_files_path = os.path.join(current_dir, "logs")

    wl_config = parse_product_config(config_path, "wl")
    wt_config = parse_product_config(config_path, "wt")

    load_logger = create_logger("load", log_files_path)
    extract_logger = create_logger("extract", log_files_path)

    cnx = connect_to_mysql({"host": DB_HOST, "user": DB_USER, 
                            "password": DB_PASSWORD, "database": DATABASE}, load_logger)
    product_type = "wl"
    product_config = wl_config
    try:
        for station_name in product_config['stations']:
            station = product_config['stations'][station_name]
            product_url = generate_product_url(station_id=station['ID'],
                                               datum=station["DATUM"],
                                               data_format=station['PRODUCT_FORMAT'],
                                               date=station['DATE'],
                                               product=product_type)

            station_info_url = generate_station_info_url(station['ID'], station["STATION_INFO_FORMAT"])

            station_info = make_get_request(station_info_url, product_config['API_HEADERS'], extract_logger, station["STATION_INFO_FORMAT"])
            station_info = transform_station_info(station_info, station["STATION_INFO_FORMAT"])

            product_data = make_get_request(product_url, product_config['API_HEADERS'], extract_logger, station['PRODUCT_FORMAT'])
            if product_type == 'wl':
                product_data = transform_water_level(product_data, station['PRODUCT_FORMAT'], station['ID'])
            elif product_type == 'wt':
                product_data = transform_water_temperature(product_data, station['PRODUCT_FORMAT'], station['ID'])

            successful_load = load_to_database(cnx=cnx, logger=load_logger, data=station_info, load_query=product_config['station_info_load_query'])
            print(product_data)
            for arr in product_data:
                successful_load = load_to_database(cnx=cnx, logger=load_logger, data=arr, load_query=product_config[f"{product_type}_load_query"])
            if successful_load:
                load_logger.info(f"Loaded data into the database for station {station_info[1]} identified by ID: {station_info[0]}")

    except Exception as e:
        extract_logger.critical(f"An unexpected error occurred: {e}")


# etl("wl", wl_config)
# etl("wt", wt_config)


if cnx and cnx.is_connected():
    cnx.close()