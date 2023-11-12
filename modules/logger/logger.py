import logging

def create_logger(logger_name, log_file_path):

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)  

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")


    file_handler = logging.FileHandler(log_file_path, mode='a')
    file_handler.setFormatter(formatter)


    logger.addHandler(file_handler)

    return logger