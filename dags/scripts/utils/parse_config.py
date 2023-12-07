from airflow.decorators import task
from airflow.operators.python import get_current_context
import logging
@task()
def parse_config(config, product):

    stations = config['API'][product]['stations']
        
    if product == "wl": product = 'water_level'
    elif product == "wt": product = 'water_temperature'
    base_url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

    product_urls, station_urls, stations_arr = [], [], []
    for station in stations:
        station = stations[station]
        stations_arr.append(station)
        product_urls.append(f"{base_url}?date={station['DATE']}&station={station['ID']}&product={product}&datum={station['DATUM']}&time_zone=gmt&units=english&format={station['PRODUCT_FORMAT']}")
        station_urls.append(f"https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{station['ID']}.{station['STATION_INFO_FORMAT']}?expand=details,products&units=english")
    
    return {'headers': config['API_HEADERS'],
            'station_urls': station_urls,
            'product_urls': product_urls,
            'stations': stations_arr}
