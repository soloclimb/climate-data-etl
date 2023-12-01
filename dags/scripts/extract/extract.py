import requests
from urllib import request
import logging
from airflow.decorators import task

@task()
def extract_data(stations, station_urls, product_urls, headers):
    station_info, product_data = [], []
    for i in range(0, len(stations)):
        
        station_data = make_get_request(station_urls[i], headers, stations[i]['STATION_INFO_FORMAT'])
        product = make_get_request(product_urls[i], headers, stations[i]['PRODUCT_FORMAT'])
        
        station_info.append(station_data)
        product_data.append(product)
        
    return {'station_info': station_info, 'product_data': product_data}
@task()
def make_get_request(url, headers, format):    
    try:
        if format == 'csv':
            res = request.urlopen(url)
            return res.read().decode('utf-8')
        
        elif format == 'json':
            res = requests.get(url=url, headers=headers)
            if res.status_code == 200:
                logging.info(f"Successfull get request to url: {url}")
                
            res.raise_for_status()
                
            return res.json()
        elif format == 'xml':
            res = request.urlopen(url)
            data = res.read().decode('utf-8')
            res.close()
            return data
        
    except requests.exceptions.Timeout as e:
        logging.error(f"Request timed out: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occured: {e}")
        return None  
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Failed to establish a connection: {e}")
        return None 
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None 
    


