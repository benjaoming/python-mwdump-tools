import logging

FORMAT = '%(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('mw-tools')
logger.setLevel(logging.DEBUG)