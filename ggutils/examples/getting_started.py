# __init__.py
#
# Copyright 2018-2020 Geek Guild Co., Ltd.
#

import sys

def hello(your_name='World'):
    function_name = sys._getframe().f_code.co_name
    return 'Hello, {}! This is module:[{}] function:[{}].'.format(your_name, __file__, function_name)

if __name__ == '__main__':
    print(hello())