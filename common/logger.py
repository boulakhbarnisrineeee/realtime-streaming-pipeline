import logging
import sys
from pythonjsonlogger import jsonlogger

def get_logger(service_name: str) -> logging.Logger:
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(name)s %(levelname)s %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger