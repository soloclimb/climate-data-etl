import csv
import xmltodict
from io import StringIO
from airflow.decorators import task

@task(multiple_outputs=True)
def transform_product(config, extracted_data, logger):
    pass

def _transform_water_level(config, product_data, logger):
    try:
        stations = config['stations']
        product_data = product_data['product_data']
        res = []
        i = 0
        for station in stations:
            station_id = station['ID']
            if station['PRODUCT_FORMAT'] == "json":
                for dct in product_data:
                    dct = dct['data'][0]
                    f = dct['f'].split(',')
                    res.append([station_id, dct['t'] + ":00", dct['v'], dct['s'], f[1], f[2], f[3]])                        

            elif station['PRODUCT_FORMAT'] == "csv":
                csv_file = StringIO(product_data[i])
                reader = csv.reader(csv_file)
                for row in reader:
                    res.append([station['ID']] + [row[x] for x in range(0, 7) if x != 3])
            
            elif station['PRODUCT_FORMAT'] == 'xml':
                data = xmltodict.parse(product_data[i])['data']['observations']['wl']
                for wl in data:
                    f = wl['@f'].split(',')
                    res.append([station_id ,wl['@t'], wl['@v'], wl['@s'], f[1], f[2], f[3]])
            
            logger.info(f"Successfully transformed water level data, station id: {station['ID']}")
            i += 1
        return res

    except (KeyError, IndexError, TypeError) as e:
        logger.error(f'Error occured while transfroming wl: {e}' )

   

def _transform_water_temperature(config, product_data, logger):
    try:
        stations = config['stations']
        product_data = product_data['product_data']
        res = []
        i = 0
        for station in stations:
            station_id = station['ID']
            if station['PRODUCT_FORMAT'] == "json":
                for dct in product_data:
                    dct = dct['data'][0]
                    f = dct['f'].split(',')
                    res.append([station_id, dct['t'] + ":00", dct['v'], f[0], f[1], f[2]])                        

            elif station['PRODUCT_FORMAT'] == "csv":
                csv_file = StringIO(product_data[i])
                reader = csv.reader(csv_file)
                for row in reader:
                    res.append([station_id] + row)
            
            elif station['PRODUCT_FORMAT'] == 'xml':
                data = xmltodict.parse(product_data[i])['data']['observations']['wt']
                for wl in data:    
                    f = wl['@f'].split(',')
                    return [[station_id ,wl['@t'], wl['@v'], f[0], f[1], f[2]]]    
            
            logger.info(f"Successfully transformed water temperature data, station id: {station['ID']}")
            i += 1
        return res

    except (KeyError, IndexError, TypeError) as e:
        logger.error(f'Error occured while transfroming water temperature data: {e}')
    
@task()
def _transform_station_info(config, stations_data, logger):
    try:
        stations = config['stations']
        stations_data = stations_data['station_info']
        res = []
        for i in range(0, len(stations)):

            products, data = [], []
            if stations[i]['STATION_INFO_FORMAT'] == "json":
                data = stations_data[i]['stations'][0]
                products = data['products']['products']
                
            elif stations[i]['STATION_INFO_FORMAT'] == 'xml':
                data = xmltodict.parse(stations_data[i])['Stations']['Station']
                products = data['products']['Product']

            arr = [data['id'], data['name'], data['lat'], data['lng'],data['state'], data['timezonecorr'], '']
            for j in range(0, len(products)):
                arr[-1] += f"{products[j]['name']}, "
            
            arr[-1] = arr[-1].rstrip(', ')
            res.append(arr)

            logger.info(f"Successfully transformed station info data, station id: {stations[i]['ID']}")
        
        return res
    
    except (KeyError, IndexError, TypeError) as e:
        logger.error(f'Error occured while transforming water temperature data: {e}')
