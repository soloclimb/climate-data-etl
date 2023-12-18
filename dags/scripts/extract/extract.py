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
    i = 0
    station_info = []
    products_data = {}
    for station in stations:
        station_info_format = stations[station]['STATION_INFO_FORMAT']
        products = stations[station]['PRODUCTS']
        try:
            for dct in products:
                product = dct['product']
                product_format = dct['format']
                logger.info(dct)
                product_data = []
                for j in range(0, len(product_urls[product])):
                    logger.info(product_urls[product][j])
                    if product_format == 'csv':
                        res = requests.get(url=product_urls[product][j], headers=headers)
                        logger.info(f"Raw response content: {res.text}")
                        product_data.append(res.content.decode('utf-8'))
                    
                    elif product_format == 'json':
                        res = requests.get(url=product_urls[product][j], headers=headers)  
                        product_data.append(res.json())

                    elif product_format == 'xml':
                        res = requests.get(url=product_urls[product][j], headers=headers)
                        product_data.append(res.content.decode('utf-8'))
                        res.close()

                    logger.info(f"Successfull get request to {product_urls[product][j]}")
                
                logger.info(product_data)
                if product in products_data:
                    products_data[product] = products_data[product] + product_data
                else:
                    products_data[product] = product_data


            if station_info_format == 'json':
                res = requests.get(url=station_urls[i], headers=headers)  
                station_info.append(res.json())
            
            elif station_info_format == 'xml':
                res = requests.get(station_urls[i])
                data = res.content.decode('utf-8')
                station_info.append(data)
            logger.info(station_info)
            logger.info(f"Successfull get request to {station_urls[i]}")
            
            i += 1

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
    


