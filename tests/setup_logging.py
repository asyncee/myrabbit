import logging


def setup_logging(level=logging.INFO):
    logging.basicConfig(level=level)
    logging.getLogger("pika").setLevel(logging.ERROR)
