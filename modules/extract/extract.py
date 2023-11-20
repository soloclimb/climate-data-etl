import requests
from urllib import request

def make_get_request(url, headers, logger, format):    
    try:
        if format == 'csv':
            res = request.urlopen(url)
            csv_data = res.read().decode('utf-8')
            return csv_data
        
        elif format == 'json':
            res = requests.get(url=url, headers=headers)
            if res.status_code == 200:
                logger.info(f"Successfull get request to url: {url}")
                
            res.raise_for_status()
                
            return res.json()
        elif format == 'xml':
            res = request.urlopen(url)
            data = res.read()
            res.close()
            return data
        
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
    
def generate_product_url(station_id, datum, data_format, date, product, 
                    time_zone="time_zone=gmt", units="units=english"):
    
    base_url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    if product == 'wt': 
        product = "water_temperature"
    elif product == "wl":
        product = "water_level"
        
    api_url = f"{base_url}?date={date}&station={station_id}&product={product}&datum={datum}&{time_zone}&{units}&format={data_format}"
    
    return api_url
def generate_station_info_url(id, info_format):
    return f"https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{id}.{info_format}?expand=details,products&units=english"
