import logging

def set_logger(log_path: str="log.txt"):
    # Create a custom logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create handlers
    file_handler = logging.FileHandler(log_path, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)

    # Create formatters and add it to handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)