import time
import logging
import logging.config
import logging.handlers
import configparser
from datetime import datetime


# base logger config setup
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M', filename='./logs/db_comparator.log', filemode='w')

# Handler writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)

# log to the root logger, or any other logger. First the root...
logging.info('DB Comparator logging')

# application:
logger = logging.getLogger('db_comparator.main')
#logger2 = logging.getLogger('myapp.area2')

# application code
logger.debug('Debug message')
logger.info('Info message')
logger.warning('Warning message')
logger.error('Error message')
logger.critical('Critical error message')