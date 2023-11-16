import json 
import csv
from io import StringIO

def transform_water_level_row(arr, dct, response_columns):
    for col in response_columns:
        if col[0] == 'f':
            arr.append(dct['f'].split(",")[int(col[2])])
        else:
            arr.append(dct[col])
    return arr
    
def transform_water_level(data, data_format, station_id):
    if data_format == "json": 
       pass
    elif data_format == "csv":
        csv_file = StringIO(data)
        reader = csv.reader(csv_file)
        arr = []
        for row in reader:
            arr.append([station_id] + [row[x] for x in range(0, 7) if x != 3])
        return arr
       
def transform_station_info(data, data_format):
    if data_format == "json":
        data = data['stations'][0]
        arr = [data['id'], data['name'], data['lat'], data['lng'],data['state'], data['timezonecorr'], '']
        products = data['products']['products']
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
      