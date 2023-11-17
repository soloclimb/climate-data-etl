import json 
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
    
def transform_water_level(data, data_format, station_id):
    if data_format == "json": 
        res = []   
        for dct in data['data']:
           f = dct['f'].split(',')
           res.append([station_id ,dct['t'], dct['v'], dct['s'], f[1], f[2], f[3]])
        return res   

    elif data_format == "csv":
        csv_file = StringIO(data)
        reader = csv.reader(csv_file)
        arr = []
        for row in reader:
            arr.append([station_id] + [row[x] for x in range(0, 7) if x != 3])
        return arr
    
    elif data_format == 'xml':
        data = xmltodict.parse(data)['data']['observations']['wl']
        f = data['@f'].split(',')
        return [[station_id ,data['@t'], data['@v'], data['@s'], f[1], f[2], f[3]]]
               
def transform_station_info(data, data_format):
    if data_format == "json":
        data = data['stations'][0]
        products = data['products']['products']
        
    elif data_format == 'xml':
        data = xmltodict.parse(data)['Stations']['Station']
        products = data['products']['Product']

    arr = [data['id'], data['name'], data['lat'], data['lng'],data['state'], data['timezonecorr'], '']
    for i in range(0, len(products)):
        arr[-1] += f"{products[i]['name']}, "
    
    arr[-1] = arr[-1].rstrip(', ')
    return arr
    
def parse_json_file(filepath):
    with open(filepath, "r") as config_file:
      try:
        return json.load(config_file)
      except json.decoder.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"File content: {config_file.read()}")
        raise
      