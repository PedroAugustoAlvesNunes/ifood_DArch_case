import logging
from sys import stdout
from datetime import datetime
import os

def config_log(log_name='log'):
  file_name = log_name
  path_log = './logs/{}_{:%Y%m%d}.log'.format(file_name, datetime.now())
  if not(os.path.isdir('./logs')):
    os.makedirs('./logs')
  fh = logging.FileHandler(path_log)
  _handlers = [fh]
  logging.basicConfig(
  level=logging.INFO,
  format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
  handlers=_handlers)
  logger = logging.getLogger(log_name)
  return logger

