import json
def parse_product_config(filepath, product):
    with open(filepath, "r") as config_file:
        try:
            config = json.load(config_file)
            columns_key = product + "_response_columns"
            query_key = product + "_load_query"


            return {
                "API_HEADERS": config["API_HEADERS"],
                "stations": config["API"][product]['stations'],
                "response_columns": config["API"].get(columns_key, None),
                f"{query_key}": config["DB_QUERIES"][query_key],
                "station_info_load_query": config["DB_QUERIES"]["station_info_load_query"]
            }

        except json.decoder.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            print(f"File content: {config_file.read()}")
            raise
