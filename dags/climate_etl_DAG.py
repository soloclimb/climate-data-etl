from airflow import DAG 
from airflow.operators.python import PythonOperator 
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.mysql_operator import MySqlOperator 
from datetime import datetime
import os 
import json
import requests
from urllib import request
import csv
from io import StringIO
# import xmltodict

def _create_config():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "../config", "config.json")
    with open (config_path, "r") as config:
        config = json.load(config)
    
    station = config['API']['wl']['stations']['Hilo, Hilo Bay, Kuhio Bay, HI']
    product = "water_level"
        
    base_url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    product_url = f"{base_url}?date={station['DATE']}&station={station['ID']}&product={product}&datum={station['DATUM']}&time_zone=gmt&units=english&format={station['PRODUCT_FORMAT']}"
    station_url = f"https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{station['ID']}.{station['STATION_INFO_FORMAT']}?expand=details,products&units=english"
    return {'headers': config["API_HEADERS"],
              'station_url': station_url,
              'product_url': product_url,
              'product_format': station["PRODUCT_FORMAT"],
              'station_info_format': station["STATION_INFO_FORMAT"],
              'station_id': station['ID']}

def _extract_data(**kwargs):
    ti = kwargs['ti']
    config = ti.xcom_pull(task_ids='create_config')
    url = config['product_url']
    format = config['product_format']
    headers = config['headers']
    try:
        if format == 'csv':
            res = request.urlopen(url)
            csv_data = res.read().decode('utf-8')
            return csv_data
        
        elif format == 'json':
            res = requests.get(url=url, headers=headers)
            if res.status_code == 200:
                print(f"Successfull get request to url: {url}")
                
            res.raise_for_status()
                
            return res.json()
        elif format == 'xml':
            res = request.urlopen(url)
            data = res.read()
            res.close()
            return data
        
    except requests.exceptions.Timeout as e:
        print(f"Request timed out: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occured: {e}")
        return None  
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to establish a connection: {e}")
        return None 
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None 

def _transform_water_level(**kwargs):
    ti = kwargs['ti']
    config = ti.xcom_pull(task_ids='create_config')
    url = config['product_url']
    data_format = config['product_format']
    headers = config['headers']
    station_id = config['station_id']
    data = ti.xcom_pull(task_ids='extract_data')
    if data_format == "json": 
        for dct in data['data']:
           f = dct['f'].split(',')
           return [station_id , str(datetime.today().strftime('%Y-%m-%d')) +" " +dct['t'] + ":00", dct['v'], dct['s'], station_id,f[1], f[2], f[3]]
        # return res   

    elif data_format == "csv":
        csv_file = StringIO(data)
        reader = csv.reader(csv_file)
        arr = []
        for row in reader:
            arr.append([station_id] + [row[x] for x in range(0, 7) if x != 3])
        return arr
    
    # elif data_format == 'xml':
    #     data = xmltodict.parse(data)['data']['observations']['wl']
    #     f = data['@f'].split(',')
    #     return [[station_id ,data['@t'], data['@v'], data['@s'], f[1], f[2], f[3]]]

def _load_to_mysql(**kwargs):
    ti = kwargs['ti']

    insert_data = ti.xcom_pull(task_ids='transform_data')
    values_str = ', '.join([str(value) for value in insert_data])
    query = f"INSERT IGNORE INTO climate_data.water_level (station_id, record_time, water_level, sigma, water_level_inferred, flat_tolerance_exceeded, expected_water_level_exceeded) VALUES ({values_str})"

    hook = MySqlHook(mysql_conn_id='climate-data-mysql')
    hook.run(query, autocommit=True)

   
with DAG("my_elt", start_date=datetime(2023, 11, 23), 
         schedule_interval="*/6 * * * *", catchup=False) as dag:
    
    create_config = PythonOperator(
            task_id='create_config',
            python_callable=_create_config
    )
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=_extract_data
    )
    transform_data = PythonOperator(
            task_id='transform_data',
            python_callable=_transform_water_level
    )
    load_to_mysql = PythonOperator(
        task_id='load_to_mysql',
        python_callable=_load_to_mysql,
        provide_context=True
    )


create_config >> extract_data >> transform_data >> load_to_mysql