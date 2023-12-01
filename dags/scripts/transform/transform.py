import csv
from io import StringIO
import xmltodict

def transform_water_level_row(arr, dct, response_columns):
    for col in response_columns:
        if col[0] == 'f':
            arr.append(dct['f'].split(",")[int(col[2])])
        else:
            arr.append(dct[col])
    return arr
    
def transform_water_level(stations, data):
    res = []
    
    i = 0
    for station in stations:
        station_id = station['ID']
        if station['PRODUCT_FORMAT'] == "json":
            for dct in data:
                dct = dct['data'][0]
                f = dct['f'].split(',')
                res.append([station_id, dct['t'] + ":00", dct['v'], dct['s'], f[1], f[2], f[3]])                        

        elif station['PRODUCT_FORMAT'] == "csv":
            csv_file = StringIO(data[i])
            reader = csv.reader(csv_file)
            for row in reader:
                res.append([station['ID']] + [row[x] for x in range(0, 7) if x != 3])
        
        elif station['PRODUCT_FORMAT'] == 'xml':
            data = xmltodict.parse(data[i])['data']['observations']['wl']
            f = data['@f'].split(',')
            res.append([station_id ,data['@t'], data['@v'], data['@s'], f[1], f[2], f[3]])
        i += 1

    return res
    # if data_format == "json": 
    #     res = []   
    #     for dct in data['data']:
    #        f = dct['f'].split(',')
    #        res.append([station_id ,dct['t'], dct['v'], dct['s'], f[1], f[2], f[3]])
    #     return res   

    # elif data_format == "csv":
    #     csv_file = StringIO(data)
    #     reader = csv.reader(csv_file)
    #     arr = []
    #     for row in reader:
    #         arr.append([station_id] + [row[x] for x in range(0, 7) if x != 3])
    #     return arr
    
    # elif data_format == 'xml':
    #     data = xmltodict.parse(data)['data']['observations']['wl']
    #     f = data['@f'].split(',')
    #     return [[station_id ,data['@t'], data['@v'], data['@s'], f[1], f[2], f[3]]]

def transform_water_temperature(stations, data):
    i = 0
    for station in stations:
        station_id = station['ID']
        res = []
        if station['PRODUCT_FORMAT'] == "json":
            for dct in data:
                dct = dct['data'][0]
                f = dct['f'].split(',')
                res.append([station_id, dct['t'] + ":00", dct['v'], f[0], f[1], f[2]])                        

        elif station['PRODUCT_FORMAT'] == "csv":
            csv_file = StringIO(data[i])
            reader = csv.reader(csv_file)
            for row in reader:
                res.append([station_id] + row)
        
        elif station['PRODUCT_FORMAT'] == 'xml':
            data = xmltodict.parse(data[i])['data']['observations']['wt']
            f = data['@f'].split(',')
            return [[station_id ,data['@t'], data['@v'], f[0], f[1], f[2]]]    
        i += 1

    return res
    # if data_format == "json": 
    #     res = []   
    #     for dct in data['data']:
    #        f = dct['f'].split(',')
    #        res.append([station_id ,dct['t'], dct['v'], f[0], f[1], f[2]])
    #     return res   

    # elif data_format == "csv":
    #     csv_file = StringIO(data)
    #     reader = csv.reader(csv_file)
    #     arr = []
    #     for row in reader:
    #         arr.append([station_id] + row)
    #     return arr
    
    # elif data_format == 'xml':
    #     data = xmltodict.parse(data)['data']['observations']['wt']
    #     f = data['@f'].split(',')
    #     return [[station_id ,data['@t'], data['@v'], f[0], f[1], f[2]]]     

def transform_station_info(stations, data):
    res = []
    for i in range(0, len(stations)):

        products, data = [], []
        if stations[i]['STATION_INFO_FORMAT'] == "json":
            data = data[i]['stations'][0]
            products = data['products']['products']
            
        elif stations[i]['STATION_INFO_FORMAT'] == 'xml':
            data = xmltodict.parse(data[i])['Stations']['Station']
            products = data['products']['Product']

        arr = [data['id'], data['name'], data['lat'], data['lng'],data['state'], data['timezonecorr'], '']
        for i in range(0, len(products)):
            arr[-1] += f"{products[i]['name']}, "
        
        arr[-1] = arr[-1].rstrip(', ')
        res.append(arr)

    return res
    # if data_format == "json":
    #     data = data['stations'][0]
    #     products = data['products']['products']
        
    # elif data_format == 'xml':
    #     data = xmltodict.parse(data)['Stations']['Station']
    #     products = data['products']['Product']

    # arr = [data['id'], data['name'], data['lat'], data['lng'],data['state'], data['timezonecorr'], '']
    # for i in range(0, len(products)):
    #     arr[-1] += f"{products[i]['name']}, "
    
    # arr[-1] = arr[-1].rstrip(', ')
    # return arr
    
