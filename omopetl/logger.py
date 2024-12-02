import logging
import os

logfile_path = os.path.join(os.getcwd(), 'project_build.log')

logging.basicConfig(
    filename=logfile_path,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger()

def log_info(message):
    logger.info(message)

def log_warning(message):
    logger.warning(message)

def log_error(message):
    logger.error(message)

def log_exception(exception):
    logger.exception("Exception occurred: %s", exception)