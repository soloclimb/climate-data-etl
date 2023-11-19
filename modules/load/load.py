import mysql.connector
import time

def connect_to_mysql(config, logger, attempts=3, delay=2):
    attempt = 1
    while attempt < attempts + 1:
        try:
            return mysql.connector.connect(**config)
        except (mysql.connector.Error, IOError) as err:
            if (attempts is attempt):
                logger.error("Failed to connect, exiting without a connection: %s", err)
                return None
            logger.warning(
                "Connection failed: %s. Retrying (%d/%d)...",
                err,
                attempt,
                attempts-1,
            )

            time.sleep(delay ** attempt)
            attempt += 1
    return None

def load_to_database(cnx, logger, data, load_query):    
    if cnx and cnx.is_connected():
        with cnx.cursor() as cursor:
            try:
                cursor.execute(load_query, data)
                cnx.commit()
                return True
            except mysql.connector.Error as e:
                logger.error(f"Failed to load, MySQL Error: {e}")
                cnx.rollback()
                return False
    else:
        logger.error("Could not connect to the database")
        return False