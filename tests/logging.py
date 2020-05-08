import logging


def setup_logging():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.ERROR)
