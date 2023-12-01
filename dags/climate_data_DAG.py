from airflow import DAG
from airflow.decorators import task

from airflow.operators.python import PythonOperator 
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime

import os 
import json
import logging    
import sys

sys.path.append('/home/soloclimb/projects/climate-data-airflow/')

from scripts.utils.parse_config import parse_config
from scripts.extract.extract import extract_data
from scripts.transform.transform import transform_station_info, transform_water_level, transform_water_temperature




def _create_config():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "../config", "config.json")
    with open (config_path, "r") as config:
        config = json.load(config)
    
    wl = parse_config(config=config, product='wl')
    wt = parse_config(config=config, product='wt')
    
    return {"headers": config["API_HEADERS"],
            "wl": wl,
            "wt": wt}
    # stations = config['API']['wl']['stations']
    # product = "water_level"
    # base_url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    # product_urls, station_urls, stations_arr = [], [], []
    # for station in stations:
    #     station = stations[station]
    #     stations_arr.append(station)
    #     product_urls.append(f"{base_url}?date={station['DATE']}&station={station['ID']}&product={product}&datum={station['DATUM']}&time_zone=gmt&units=english&format={station['PRODUCT_FORMAT']}")
    #     station_urls.append(f"https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{station['ID']}.{station['STATION_INFO_FORMAT']}?expand=details,products&units=english")
    # return {'headers': config["API_HEADERS"],
    #           'station_urls': station_urls,
    #           'product_urls': product_urls,
    #           'stations': stations_arr}


def _extract_data(**kwargs):
    ti = kwargs['ti']
    config = ti.xcom_pull(task_ids='create_config')
    headers = config['headers']
    
    wl = extract_data(config['wl']['stations'], config['wl']['station_urls'], config['wl']['product_urls'], headers)
    wt = extract_data(config['wt']['stations'], config['wt']['station_urls'], config['wt']['product_urls'], headers)

    return {'wl': wl, 'wt': wt}
    # headers = config['headers']
    # stations = config['stations']
    # station_urls = config['station_urls']
    # product_urls = config['product_urls']

    # station_info, product_data = [], []
    # for i in range(0, len(stations)):
        
    #     station_data = make_get_request(station_urls[i], headers, stations[i]['STATION_INFO_FORMAT'])
    #     product = make_get_request(product_urls[i], headers, stations[i]['PRODUCT_FORMAT'])
        
    #     station_info.append(station_data)
    #     product_data.append(product)
        # station_url = station_urls[i]
        # product_url = product_urls[i]
        # product_format = stations[i]['PRODUCT_FORMAT']
        # station_info_format = stations[i]['STATION_INFO_FORMAT']
        # try:
            
        #     if station_info_format == 'csv':
        #         res = request.urlopen(station_url)
        #         station_info.append(res.read().decode('utf-8'))
                
        #     elif station_info_format == 'json':
            
        #         res = requests.get(url=station_url, headers=headers)
        #         station_info.append(res.json())
           
        #     elif station_info_format == 'xml':
        #         res = request.urlopen(station_url)
        #         station_info.append(res.read().decode('utf-8'))
        #         res.close()

            
        #     if product_format == 'csv':
        #         res = request.urlopen(product_url)
        #         product_data.append(res.read().decode('utf-8'))
            
        #     elif product_format == 'json':
        #         res = requests.get(url=product_url, headers=headers)
        #         product_data.append(res.json())

        #     elif product_format == 'xml':
        #         res = request.urlopen(product_url)
        #         product_data.append(res.read().decode('utf-8'))
        #         res.close()
        
        # except requests.exceptions.Timeout as e:
        #     logging.error(f"Request timed out: {e}")
        # except requests.exceptions.HTTPError as e:
        #     logging.error(f"HTTP error occured: {e}")  
        # except requests.exceptions.ConnectionError as e:
        #     logging.error(f"Failed to establish a connection: {e}") 
        # except requests.exceptions.RequestException as e:
        #     logging.error(f"Request failed: {e}") 
        
    # return {'station_info': station_info, 'product_data': product_data}




def _transform_station_info(**kwargs):
    ti = kwargs['ti']
    config = ti.xcom_pull(task_ids='create_config')
    # stations = config['stations']
    extracted_data = ti.xcom_pull(task_ids='extract_data')

    wl = transform_station_info(config['wl']['stations'], extracted_data['wl']['station_info'])
    wt = transform_station_info(config['wt']['stations'], extracted_data['wt']['station_info'])

    return {'wl': wl, 'wt': wt}

    # res = []
    # for i in range(0, len(stations)):

    #     products, data = [], []
    #     if stations[i]['STATION_INFO_FORMAT'] == "json":
    #         data = station_info[i]['stations'][0]
    #         products = data['products']['products']
            
    #     elif stations[i]['STATION_INFO_FORMAT'] == 'xml':
    #         data = xmltodict.parse(station_info[i])['Stations']['Station']
    #         products = data['products']['Product']

    #     arr = [data['id'], data['name'], data['lat'], data['lng'],data['state'], data['timezonecorr'], '']
    #     for i in range(0, len(products)):
    #         arr[-1] += f"{products[i]['name']}, "
        
    #     arr[-1] = arr[-1].rstrip(', ')
    #     res.append(arr)

    # return res

def _transform_water_level(**kwargs):
    ti = kwargs['ti']
    config = ti.xcom_pull(task_ids='create_config')
    extracted_data = ti.xcom_pull(task_ids='extract_data')


    wl = transform_water_level(config['wl']['stations'], extracted_data['wl']['product_data'])
    return wl

    # i = 0
    # for station in stations:
    #     station_id = station['ID']
    #     res = []
    #     if station['PRODUCT_FORMAT'] == "json":
    #         for dct in data:
    #             dct = dct['data'][0]
    #             f = dct['f'].split(',')
    #             res.append([station_id, dct['t'] + ":00", dct['v'], dct['s'], f[1], f[2], f[3]])                        

    #     elif station['PRODUCT_FORMAT'] == "csv":
    #         csv_file = StringIO(data[i])
    #         reader = csv.reader(csv_file)
    #         for row in reader:
    #             res.append([station['ID']] + [row[x] for x in range(0, 7) if x != 3])
        
    #     elif station['PRODUCT_FORMAT'] == 'xml':
    #         data = xmltodict.parse(data[i])['data']['observations']['wl']
    #         f = data['@f'].split(',')
    #         res.append([station_id ,data['@t'], data['@v'], data['@s'], f[1], f[2], f[3]])
    #     i += 1
    # return res

def _tranform_water_temperature(**kwargs):
    ti = kwargs['ti']
    config = ti.xcom_pull(task_ids='create_config')
    extracted_data = ti.xcom_pull(task_ids='extract_data')

    wt = transform_water_temperature(config['wt']['stations'], extracted_data['wt']['product_data'])

    return wt
    
def _load_station_info(**kwargs):
    ti = kwargs['ti']

    insert_data = list(ti.xcom_pull(task_ids='transform_station_info'))
    query = "INSERT IGNORE INTO climate_data.station_info (station_id, name, lat, lon, state, timezone, products) VALUES (%s, %s, %s, %s, %s, %s, %s)"

    hook = MySqlHook(mysql_conn_id='climate-data-mysql')
    conn = hook.get_conn()
    cursor = conn.cursor()
    for arr in insert_data:

        cursor.execute(query, arr)
        conn.commit()


    logging.info(insert_data)
    cursor.close()
    conn.close()

def _load_wl(**kwargs):
    ti = kwargs['ti']

    insert_data = list(ti.xcom_pull(task_ids='transform_wl'))
    query = "INSERT IGNORE INTO climate_data.water_level (station_id, record_time, water_level, sigma, water_level_inferred, flat_tolerance_exceeded, expected_water_level_exceeded) VALUES (%s, %s, %s, %s, %s, %s, %s)"

    hook = MySqlHook(mysql_conn_id='climate-data-mysql')
    conn = hook.get_conn()
    cursor = conn.cursor()
    for arr in insert_data:

        cursor.execute(query, arr)
        conn.commit()


    logging.info(insert_data)
    cursor.close()
    conn.close()
   
with DAG("climate_data_DAG", start_date=datetime(2023, 11, 30), 
         schedule_interval="*/6 * * * *", catchup=False) as dag:
    
    create_config = PythonOperator(
            task_id='create_config',
            python_callable=_create_config
    )
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=_extract_data
    )
    transform_station_info = PythonOperator(
            task_id='transform_station_info',
            python_callable=_transform_station_info
    )
    transform_wl = PythonOperator(
            task_id='transform_wl',
            python_callable=_transform_water_level
    )
    load_station_info = PythonOperator(
        task_id='load_station_info',
        python_callable=_load_station_info,
        provide_context=True
    )
    load_wl = PythonOperator(
        task_id='load_wl',
        python_callable=_load_wl,
        provide_context=True
    )


create_config >> extract_data >> [transform_station_info, transform_wl] >> load_station_info >> load_wl