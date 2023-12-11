from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime,  timedelta

import os 
import json
import logging    
import sys

sys.path.append('/home/soloclimb/projects/climate-data-airflow/')

from scripts.utils.utils import parse_config
from scripts.extract.extract import _extract_data
from scripts.transform.transform import _transform_station_info, _transform_water_level, _transform_water_temperature


logging_path = './dags/logs/'


default_args = {
    "owner": 'soloclimb',
    "retires": 2,
    "retry_delay": timedelta(minutes=4)
}


@dag("climate_data_DAG_v11", start_date=datetime(2023, 11, 30), 
         schedule_interval="*/6 * * * *", catchup=False)
def climate_data_etl():
    def create_logger(name, logging_dest_path):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        logging_dest_path = os.path.join(logging_dest_path, name + '.log')

        file_handler = logging.FileHandler(logging_dest_path, 'a')
        file_handler.setFormatter(formatter)    

        logger.addHandler(file_handler)
        return logger 

    @task(multiple_outputs=True)
    def create_config():
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, "../config", "config.json")
        with open (config_path, "r") as config:
            return json.load(config)

    @task()    
    def load_into_db(logger, insert_data, query, if_station_data=None):
        if if_station_data:
            insert_data = insert_data + if_station_data
        hook = MySqlHook(mysql_conn_id='climate-data-mysql')
        conn = hook.get_conn()
        cursor = conn.cursor()
        for arr in insert_data:

            cursor.execute(query, arr)
            conn.commit()


        logger.info("Successfully loaded data to database")
        cursor.close()
        conn.close()

    config = create_config()      
    wl_config = parse_config(config=config, product='wl')
    wt_config = parse_config(config=config, product='wt')

    extract_logger = create_logger('extract', logging_path)
    wl_data = _extract_data(wl_config, extract_logger)
    wt_data = _extract_data(wt_config, extract_logger)

    transform_logger = create_logger('transform', logging_path)
    wl_station_info = _transform_station_info(wl_config, wl_data, transform_logger)
    wt_station_info = _transform_station_info(wt_config, wt_data, transform_logger)
    
    water_level_data = _transform_water_level(wl_config, wl_data, transform_logger)
    water_temperature_data = _transform_water_temperature(wt_config, wt_data, transform_logger)
    
    load_logger = create_logger('load', logging_path)
    load_into_db(logger=load_logger, insert_data=wl_station_info, if_station_data=wt_station_info, query="INSERT IGNORE INTO climate_data.station_info (station_id, name, lat, lon, state, timezone, products) VALUES (%s, %s, %s, %s, %s, %s, %s)")
    load_into_db(logger=load_logger, insert_data=water_level_data, query="INSERT IGNORE INTO climate_data.water_level (station_id, record_time, water_level, sigma, water_level_inferred, flat_tolerance_exceeded, expected_water_level_exceeded) VALUES (%s, %s, %s, %s, %s, %s, %s)")
    load_into_db(logger=load_logger, insert_data=water_temperature_data, query="INSERT IGNORE INTO climate_data.water_temperature (station_id, record_time, water_temperature, max_conductivity_exceeded, min_conductivity_exceeded, change_tolerance_limit_exceeded) VALUES (%s, %s, %s, %s, %s, %s)")

dag_instance = climate_data_etl()