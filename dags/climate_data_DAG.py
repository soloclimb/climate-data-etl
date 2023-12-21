from airflow.decorators import task, dag
from airflow.operators.python import get_current_context

from datetime import datetime,  timedelta

import os 
import json
import logging    
import sys

sys.path.append('/home/soloclimb/projects/climate-data-airflow/')

from scripts.utils.utils import check_data_inventory
from scripts.extract.extract import _extract_data
from scripts.transform.transform import _transform_station_info, transform_product
from scripts.load.load import load

logging_path = './dags/logs/'


default_args = {
    "owner": 'soloclimb',
    "retires": 2,
    "retry_delay": timedelta(minutes=4)
}


@dag("climate_data_DAG", start_date=datetime(2023, 11, 30), 
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
            config = json.load(config)
            return {'headers': config['API_HEADERS'],
                'stations': config['API']['stations'],
                'load_queries': config['LOAD_QUERIES']}
        
    extract_logger = create_logger('extract', logging_path)

    config = create_config()
    config = check_data_inventory(config, extract_logger)      

    extracted_data = _extract_data(config, extract_logger)

    transform_logger = create_logger('transform', logging_path)
    
    stations_info = _transform_station_info(config, extracted_data, transform_logger)
    
    products_data = transform_product(config, extracted_data['product_data'], transform_logger)

    load_logger = create_logger('load', logging_path)

    load(logger=load_logger, stations_info=stations_info, products_data=products_data, load_queries=config['load_queries'])

dag_instance = climate_data_etl()