# setup.py
#
# Copyright 2018-2020 Geek Guild Co., Ltd.
#

from distutils.core import setup

setup(
    name='ggutils',
    version='0.0.5',
    description='Python Utilities by Geek Guild',
    author='Geek Guild Co., Ltd.',
    author_email='info@geek-guild.jp',
    url='https://www.geek-guild.jp',
    package_dir={
        'ggutils': 'ggutils',
        'ggutils.examples': 'ggutils/examples',
        'ggutils.test': 'ggutils/test'
    },
    packages=['ggutils', 'ggutils.examples', 'ggutils.test']
)
