import csv
import xmltodict
from io import StringIO
from airflow.decorators import task
from typing import Dict, List, Union
import logging

ConfigData = Dict[str, Union[List[Dict[str, str]], Dict[str, str]]]
ExtractedData = Dict[str, Union[List[str], List[Dict[str, Union[str, str]]]]]
LoggerType = logging.Logger
TransformedData = Dict[str, Union[List[str], List[Dict[str, Union[str, str]]]]]
TransformedProductData = List[List[Union[str, int, float]]]
StationData = List[List[Union[str, int, float]]]
StationInfoData = List[List[Union[str, int, float]]]
ProductData = List[Union[str, str]]
StationsData = Dict[str, Union[str, List[Dict[str, Union[str, str]]]]]

@task(multiple_outputs=True)
def transform_product(config: ConfigData, extracted_data: ExtractedData, logger: LoggerType) -> TransformedData:
    for product, product_data in extracted_data.items():
        if product == 'water_level':
            extracted_data['water_level'] = _transform_water_level(config, extracted_data['water_level'], logger)
        elif product == 'water_temperature':
            extracted_data['water_temperature'] =  _transform_water_temperature(config, extracted_data['water_temperature'], logger)
    return extracted_data
def _transform_water_level(config: ConfigData, product_data: ProductData, logger: LoggerType) -> TransformedProductData:
    try:
        stations = config['stations']
        res = []
        i = 0
        for station in stations:
            station_id = stations[station]['ID']
            products = stations[station]['PRODUCTS']
            for dct in products:
                if dct['product'] == 'water_level':
                    product_format = dct['format']
                    if product_format == "json":
                        for dct in product_data:
                            dct = dct['data'][0]
                            f = dct['f'].split(',')
                            res.append([station_id, dct['t'] + ":00", dct['v'], dct['s'], f[1], f[2], f[3]])                        

                    elif product_format == "csv":
                        csv_file = StringIO(product_data[i])
                        reader = csv.reader(csv_file)
                        for row in reader:
                            res.append([station_id] + [row[x] for x in range(0, 7) if x != 3])
                    
                    elif product_format == 'xml':
                        data = xmltodict.parse(product_data[i])['data']['observations']['wl']
                        for wl in data:
                            f = wl['@f'].split(',')
                            res.append([station_id ,wl['@t'], wl['@v'], wl['@s'], f[1], f[2], f[3]])
                
            logger.info(f"Successfully transformed water level data, station id: {station_id}")
            i += 1
        logger.info(f"returned value: {res}")
        return res

    except (KeyError, IndexError, TypeError) as e:
        logger.error(f'Error occured while transfroming wl: {e}' )

   

def _transform_water_temperature(config: ConfigData, product_data: ProductData, logger: LoggerType) -> TransformedProductData:    
    res = []
    try:
        stations = config['stations']
        i = 0
        for station in stations:
            station_id = stations[station]['ID']
            products = stations[station]['PRODUCTS']
            for dct in products:
                if dct['product'] == 'water_temperature':
                    product_format = dct['format']
                    if product_format == "json":
                        for dct in product_data[i]['data']:
                            f = dct['f'].split(',')
                            res.append([station_id, dct['t'] + ":00", dct['v'], f[0], f[1], f[2]])                        

                    elif product_format == "csv":
                        csv_file = StringIO(product_data[i])
                        reader = csv.reader(csv_file)
                        for row in reader:
                            res.append([station_id] + row)
                    
                    elif product_format == 'xml':
                        data = xmltodict.parse(product_data[i])['data']['observations']['wt']
                        for wl in data:    
                            f = wl['@f'].split(',')
                            return [[station_id ,wl['@t'], wl['@v'], f[0], f[1], f[2]]]    
            
            logger.info(f"Successfully transformed water temperature data, station id: {station_id}")
            i += 1
        logger.info(f"returned value: {res}")

    except (KeyError, IndexError, TypeError) as e:
        logger.error(f'Error occured while transfroming water temperature data: {e}')
    
    return res
@task()
def _transform_station_info(config: ConfigData, stations_data: StationsData, logger: LoggerType) -> StationInfoData:    
    try:
        stations = config['stations']
        stations_data = stations_data['station_info']
        res = []
        i  = 0
        for station in stations:

            products, data = [], []
            if stations[station]['STATION_INFO_FORMAT'] == "json":
                data = stations_data[i]['stations'][0]
                products = data['products']['products']
                
            elif stations[station]['STATION_INFO_FORMAT'] == 'xml':
                data = xmltodict.parse(stations_data[i])['Stations']['Station']
                products = data['products']['Product']

            arr = [data['id'], data['name'], data['lat'], data['lng'],data['state'], data['timezonecorr'], '']

            for j in range(0, len(products)):
                arr[-1] += f"{products[j]['name']}, "
            
            arr[-1] = arr[-1].rstrip(', ')
            res.append(arr)
            logger.info(f"Successfully transformed station info data, station id: {stations[station]['ID']}")
            i += 1
        
        return res
    
    except (KeyError, IndexError, TypeError) as e:
        logger.error(f'Error occured while transforming station info data: {e}')
