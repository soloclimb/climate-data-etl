

def transform_water_level_row(arr, dct, response_columns):
    for col in response_columns:
        if col[0] == 'f':
            arr.append(dct['f'].split(",")[int(col[2])])
        else:
            arr.append(dct[col])