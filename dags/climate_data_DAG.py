from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime,  timedelta

import os 
import json
import logging    
import sys

sys.path.append('/home/soloclimb/projects/climate-data-airflow/')

from scripts.utils.parse_config import parse_config
from scripts.extract.extract import _extract_data
from scripts.transform.transform import _transform_station_info, _transform_water_level, _transform_water_temperature

default_args = {
    "owner": 'soloclimb',
    "retires": 2,
    "retry_delay": timedelta(minutes=4)
}


@dag("climate_data_DAG_v05", start_date=datetime(2023, 11, 30), 
         schedule_interval="*/6 * * * *", catchup=False)
def climate_data_etl():
    @task(multiple_outputs=True)
    def create_config():
        context = get_current_context()
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, "../config", "config.json")
        with open (config_path, "r") as config:
            config = json.load(config)
        
        wl = parse_config(config=config, product='wl')
        wt = parse_config(config=config, product='wt')
        wl = {
            'station_urls': wl['station_urls'],
            'product_urls': wl['product_urls'],
            'stations': wl['stations_arr']
        }
        wt = {
            'station_urls': wt['station_urls'],
            'product_urls': wt['product_urls'],
            'stations': wt['stations_arr']
        }
        return {"headers": config["API_HEADERS"],
                "wl": wl, 
                "wt": wt}

    @task(multiple_outputs=True)
    def extract_data(config):
        headers = config['headers']
        
        wl = _extract_data(config['wl']['stations'], config['wl']['station_urls'], config['wl']['product_urls'], headers)
        wt = _extract_data(config['wt']['stations'], config['wt']['station_urls'], config['wt']['product_urls'], headers)

        return {'wl': wl, 'wt': wt}



    @task(multiple_outputs=True)
    def transform_station_info(config, extracted_data):
        wl = _transform_station_info(config['wl']['stations'], extracted_data['wl']['station_info'])
        wt = _transform_station_info(config['wt']['stations'], extracted_data['wt']['station_info'])
        return {'wl': wl, 'wt': wt}

    @task()
    def transform_water_level(config, extracted_data):
        wl = _transform_water_level(config['wl']['stations'], extracted_data['wl']['product_data'])
        return wl

    @task()
    def transform_water_temperature(config, extracted_data):
        wt = _transform_water_temperature(config['wt']['stations'], extracted_data['wt']['product_data'])

        return wt
    @task()    
    def load_into_db(insert_data, query):

        hook = MySqlHook(mysql_conn_id='climate-data-mysql')
        conn = hook.get_conn()
        cursor = conn.cursor()
        for arr in insert_data:

            cursor.execute(query, arr)
            conn.commit()


        logging.info(insert_data)
        cursor.close()
        conn.close()
    config = create_config()
    extracted_data = extract_data(config=config)
    station_info_data = transform_station_info(config, extracted_data)
    water_level_data = transform_water_level(config, extracted_data)
    water_temperature_data = transform_water_temperature(config, extracted_data)
    load_into_db(station_info_data, "INSERT IGNORE INTO climate_data.station_info (station_id, name, lat, lon, state, timezone, products) VALUES (%s, %s, %s, %s, %s, %s, %s)")
    load_into_db(water_level_data, "INSERT IGNORE INTO climate_data.water_level (station_id, record_time, water_level, sigma, water_level_inferred, flat_tolerance_exceeded, expected_water_level_exceeded) VALUES (%s, %s, %s, %s, %s, %s, %s)")
    load_into_db(water_temperature_data, "INSERT IGNORE INTO climate_data.water_temperature (station_id, record_time, water_temperature, max_conductivity_exceeded, min_conductivity_exceeded, change_tolerance_limit_exceeded) VALUES (%s, %s, %s, %s, %s, %s)")

dag_instance = climate_data_etl()