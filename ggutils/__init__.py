# __init__.py
#
# Copyright 2018-2020 Geek Guild Co., Ltd.
#

import logging
import logging.config as logging_config

def get_logger(fname=None):
    '''
    The function to get logger
    :param fname: config file name
    :return: logger of logging module
    '''
    # Get config file from [fname, 'logging.conf' 'ggutils_logging.ini']
    fname_src_list = [fname, 'logging.conf' 'ggutils_logging.ini']
    for fname in fname_src_list:
        if fname is None:
            continue
        try:
            # construct logger module
            print('try to get_logger_module from config file: {}'.format(fname))
            logging.config.fileConfig(fname)
            logger = logging.getLogger()
            logger.info('DONE get_logger_module from config file: {}'.format(fname))
        except (KeyError, FileNotFoundError) as e:
            print('Faild to get_logger_module from config file: {} with error: {}'.format(fname, e))
            continue
    logger = logging.getLogger(__name__)
    logger.info('get_logger_module default logger')

    return logger

def get_module_logger(fname=None):
    '''
    This function is renamed to ```get_logger```
    '''
    return get_logger(fname)

def get_logger_module(fname=None):
    '''
    This function is renamed to ```get_logger```
    '''
    return get_logger(fname)
