
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
            res = requests.get(url, headers=headers)
            print(str(res.content))
            return res.content
        
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
    
def generate_water_level_url(station_id, datum, data_format, date="today", product="product=water_level", 
                    time_zone="time_zone=gmt", units="units=english"):
    
    base_url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    
    api_url = f"{base_url}?date={date}&station={station_id}&{product}&datum={datum}&{time_zone}&{units}&format={data_format}"
    
    return api_url