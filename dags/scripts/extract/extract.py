import requests
from urllib import request
import logging
from airflow.decorators import task

@task(multiple_outputs=True)
def _extract_data(config, logger):
    stations = config['stations']
    station_urls = config['station_urls']
    product_urls = config['product_urls']
    headers = config['headers']

    station_info, product_data = [], []
    for i in range(0, len(stations)):
        station_info_format = stations[i]['STATION_INFO_FORMAT']
        product_format = stations[i]['PRODUCT_FORMAT']
        try:
            if product_format == 'csv':
                res = request.urlopen(product_urls[i])
                product_data.append(res.read().decode('utf-8'))
            
            elif product_format == 'json':
                res = requests.get(url=product_urls[i], headers=headers)  
                product_data.append(res.json())
            
            elif product_format == 'xml':
                res = request.urlopen(product_urls[i])
                data = res.read().decode('utf-8')
                res.close()
                product_data.append(data)

            logger.info(f"Successfull get request to {product_urls[i]}")

            if station_info_format == 'json':
                res = requests.get(url=station_urls[i], headers=headers)  
                station_info.append(res.json())
            
            elif station_info_format == 'xml':
                res = request.urlopen(station_urls[i])
                data = res.read().decode('utf-8')
                res.close()
                station_info.append(data)
            
            logger.info(f"Successfull get request to {station_urls[i]}")

        except requests.exceptions.Timeout as e:
            logger.error(f"Request timed out: {e}")
            return None
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occured: {e}")
            return None  
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Failed to establish a connection: {e}")
            return None 
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None 
        
    return {'station_info': station_info, 'product_data': product_data}

def make_get_request(url, headers, format, logger):    
    try:
        if format == 'csv':
            res = request.urlopen(url)
            return res.read().decode('utf-8')
        
        elif format == 'json':
            res = requests.get(url=url, headers=headers)
            return res.json()
        
        elif format == 'xml':
            res = request.urlopen(url)
            data = res.read().decode('utf-8')
            res.close()
            return data
        
        logger.info(f"Successfull get request to {url}")

        
    except requests.exceptions.Timeout as e:
        logger.error(f"Request timed out: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error occured: {e}")
        return None  
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Failed to establish a connection: {e}")
        return None 
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None 
    


