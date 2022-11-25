import logging
import sys

from pythonjsonlogger import jsonlogger

logHandler = logging.StreamHandler(stream=sys.stdout)
logHandler.setLevel(level=logging.INFO)

formatter = jsonlogger.JsonFormatter(
    fmt="%(levelname)s %(message)s %(thread)d %(name)s",
    timestamp=True,
)

logHandler.setFormatter(formatter)

logger = logging.getLogger()
logger.handlers.clear()
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)
