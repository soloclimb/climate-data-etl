import json 
import requests


def parse_json_file(filepath):
    with open(filepath, "r") as config_file:
      try:
        return json.load(config_file)
      except json.decoder.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"File content: {config_file.read()}")
        raise
      

def make_get_request(url, headers, logger):
   
    try:
        res = requests.get(url=url, headers=headers)
        if res.status_code == 200:
            logger.info(f"Successfull get request to url: {url}")
        
        res.raise_for_status()
        return res.json()  

    except requests.exceptions.Timeout as e:
        logger.error(f"Request timed out: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error occured: {e}")
        return None  
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Failed to establish a connection: {e}")
        return None 
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None 