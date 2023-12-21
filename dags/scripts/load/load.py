from airflow.hooks.mysql_hook import MySqlHook
from airflow.decorators import task


def insert_into_db(logger, insert_data, query):
    hook = MySqlHook(mysql_conn_id='climate-data-mysql')
    conn = hook.get_conn()
    cursor = conn.cursor()
    for arr in insert_data:

        cursor.execute(query, arr)
        conn.commit()


    logger.info("Successfully loaded data to database")
    cursor.close()
    conn.close()

@task()
def load(load_queries, stations_info, products_data, logger):
    insert_into_db(logger, stations_info, load_queries["station_info"])
    for product in products_data:
        insert_into_db(logger, products_data[product], load_queries[product])
