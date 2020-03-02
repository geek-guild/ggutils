# __init__.py
#
# Copyright 2018-2020 Geek Guild Co., Ltd.
#

import logging

def get_logger_module(fname=None):
    fname = fname or 'logging.conf'
    # construct logger module
    logging.config.fileConfig(fname)
    logger = logging.getLogger()

    return logger