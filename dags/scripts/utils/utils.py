from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.hooks.mysql_hook import MySqlHook

@task()
def parse_config(config):

    stations = config['API']['stations']
        
    if product == "wl": product = 'water_level'
    elif product == "wt": product = 'water_temperature'
    base_url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

    product_urls, station_urls, stations_arr = [], [], []
    for station in stations:
        station = stations[station]
        stations_arr.append(station)
        product_urls.append(f"{base_url}?date={station['DATE']}&station={station['ID']}&product={product}&datum={station['DATUM']}&time_zone=gmt&units=english&format={station['PRODUCT_FORMAT']}")
    
    return {'headers': config['API_HEADERS'],
            'station_urls': station_urls,
            'product_urls': product_urls,
            'stations': stations_arr}


@task(multiple_outputs=True)
def check_data_inventory(config):
    product_urls, station_urls = [], []
    hook = MySqlHook(mysql_conn_id='climate-data-mysql')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SHOW COLUMNS FROM climate_data.data_inventory")
    columns = cursor.fetchall()

    data_inventory = [column[0] for column in columns]
    for station in config['stations']:

        station = config[station]['stations']
        station_urls.append(f"https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{station['ID']}.{station['STATION_INFO_FORMAT']}?expand=details,products&units=english")

        station_products = station['PRODUCTS']
        cursor.execute(f"SELECT * FROM climate_data.data_inventory WHERE id = {station['ID']}")
        available_products = cursor.fetchall()
        for dct in station_products:
            if dct["product"] in data_inventory:
                if available_products and len(available_products) > 0:
                    if dct["product"] in available_products:
                        product_urls.append(f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?date={station['DATE']}&station={station['ID']}&product={dct['product']}&datum={station['DATUM']}&time_zone=gmt&units=english&format={dct['format']}")


    cursor.close()
    conn.close()