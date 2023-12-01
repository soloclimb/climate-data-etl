import json
import os
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
    
    return {'headers': config["API_HEADERS"],
              'station_urls': station_urls,
              'product_urls': product_urls,
              'stations': stations_arr}
# def parse_product_config(filepath, product):
#     with open(filepath, "r") as config_file:
#         try:
#             config = json.load(config_file)
#             columns_key = product + "_response_columns"
#             query_key = product + "_load_query"


#             return {
#                 "API_HEADERS": config["API_HEADERS"],
#                 "stations": config["API"][product]['stations'],
#                 "response_columns": config["API"].get(columns_key, None),
#                 f"{query_key}": config["DB_QUERIES"][query_key],
#                 "station_info_load_query": config["DB_QUERIES"]["station_info_load_query"]
#             }

#         except json.decoder.JSONDecodeError as e:
#             print(f"Error decoding JSON: {e}")
#             print(f"File content: {config_file.read()}")
#             raise
