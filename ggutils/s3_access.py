# s3_access.py
# S3 Access module
# Copyright 2018-2020 Geek Guild Co., Ltd.
#

import boto3
import os
from pathlib import Path
import argparse

from ggutils import get_logger_module

logger = get_logger_module()

def download(s3_bucket_name, s3_key=None, local_root_dir='./', local_file_path=None):

    if s3_key is None: s3_key = ''
    s3_key = str(s3_key)

    # filter object keys with s3_key
    obj_not_found = True
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket_name)
    for obj in bucket.objects.filter(Prefix=s3_key):
        logger.debug('s3_bucket_name: {}, obj.key: {}'.format(s3_bucket_name, obj.key))
        _download_one_file(s3_bucket_name=s3_bucket_name, s3_key=obj.key,
                           local_root_dir=local_root_dir, local_file_path=local_file_path)
        obj_not_found = False
    if obj_not_found:
        logger.warn('No object with s3_bucket_name: {}, s3_key: {}'.format(s3_bucket_name, s3_key))
        return


def _download_one_file(s3_bucket_name, s3_key, local_root_dir='./', local_file_path=None):

    logger.debug('TODO _download_one_file file from s3_bucket_name: {}, s3_key: {} to local_root_dir: {}, local_root_dir: {}'.format(s3_bucket_name, s3_key, local_root_dir, local_root_dir))

    if local_file_path is None: local_file_path = ''
    if s3_key is not None and len(local_file_path) == 0:
        local_file_path = s3_key

    local_root_dir = (str(local_root_dir) + '/').replace('//', '/')
    local_path = os.path.join(local_root_dir, local_file_path)
    os.makedirs(Path(local_path).parent, exist_ok=True)

    s3 = boto3.resource('s3')
    s3.Bucket(s3_bucket_name).download_file(s3_key, local_path)
    logger.debug('DONE _download_one_file file:')


def upload(s3_bucket_name, s3_key=None, local_root_dir='./', local_file_path=None):

    if s3_key is None: s3_key = ''
    s3_key = str(s3_key)
    if local_file_path is None: local_file_path = ''
    local_file_path = str(local_file_path)

    # check local_root_dir and local_file_path
    local_root_dir = (str(local_root_dir) + '/').replace('//', '/')
    _local_path = os.path.join(local_root_dir, local_file_path)

    assert os.path.exists(_local_path)
    if os.path.isfile(_local_path):
        logger.debug('_local_path: {} is file. TODO upload the file'.format(_local_path))
        _upload_one_file(s3_bucket_name, s3_key, local_root_dir, local_file_path)
        return
    elif os.path.isdir(_local_path):
        logger.debug('_local_path: {} is directory. TODO upload the files in the directory'.format(_local_path))
        # scan all files under local_file_path
        _local_file_path_list = [fpath.split(local_root_dir)[1] for fpath in list(_find_all_files_and_dirs(_local_path)) if os.path.isfile(fpath)]
        logger.debug('_local_file_path_list (files only): {}'.format(_local_file_path_list))
        for _local_file_path in _local_file_path_list:
            _upload_one_file(s3_bucket_name, s3_key, local_root_dir, local_file_path=_local_file_path)
        return
    else:
        ValueError('Invarid file path: {}'.format(_local_path))


def _upload_one_file(s3_bucket_name, s3_key=None, local_root_dir='./', local_file_path=None):
    logger.debug('TODO _upload_one_file file from local_root_dir: {}, local_file_path: {} to s3_bucket_name: {}, s3_key: {}'.format(
        local_root_dir, local_file_path, s3_bucket_name, s3_key))

    if local_file_path is None: local_file_path = ''
    if local_file_path is not None and len(s3_key) == 0:
        s3_key = local_file_path

    local_root_dir = (str(local_root_dir) + '/').replace('//', '/')
    local_path = os.path.join(local_root_dir, local_file_path)
    s3_client = boto3.client('s3')
    s3_client.upload_file(local_path, s3_bucket_name, s3_key)
    logger.debug('DONE _upload_one_file file:')


def _find_all_files_and_dirs(target_directory):
    for root, dirs, files in os.walk(target_directory):
        yield root
        for file in files:
            yield os.path.join(root, file)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='tsp')
    parser.add_argument('--proc_type', '-pt', type=str, required = True,
                        help='Types of access processing. (download or upload)')
    parser.add_argument('--s3_bucket_name', '-bn', type=str, required = True,
                        help='s3_bucket_name')
    parser.add_argument('--s3_key', '-key', type=str, default=None,
                        help='s3_key')
    parser.add_argument('--local_root_dir', '-lrd', type=str, default=None,
                        help='local_root_dir')
    parser.add_argument('--local_file_path', '-lfp', type=str, default=None,
                        help='local_file_path')

    args = parser.parse_args()
    if args.proc_type == 'download':
        download(args.s3_bucket_name, args.s3_key, args.local_root_dir, args.local_file_path)
    elif args.proc_type == 'upload':
        upload(args.s3_bucket_name, args.s3_key, args.local_root_dir, args.local_file_path)
    else:
        KeyError('Invalid proc_type: {}'.format(args.proc_type))




