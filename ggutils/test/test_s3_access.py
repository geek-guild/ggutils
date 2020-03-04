# test_s3_access.py
# Test class for S3 Access module
#
# Copyright 2018-2020 Geek Guild Co., Ltd.
#
# usage
# cd $REPOSITORY_PARENT_DIR/ggutils/ggutils/
# pytest -v test/test_s3_access.py -k "test_download_one_file"
# pytest -v test/test_s3_access.py -k "test_download_files"
# pytest -v test/test_s3_access.py -k "test_upload_one_file"
# pytest -v test/test_s3_access.py -k "test_upload_files"
# pytest -v test/test_s3_access.py -k "test_upload_all_files"
# pytest -v test/test_s3_access.py -k "test_all"

import os
import shutil
import time
import boto3
import hashlib

from ggutils import get_logger_module
from ggutils import s3_access
logger = get_logger_module()

# Change TEST_BUCKET_NAME for your enviroments
TEST_BUCKET_NAME = 'example.bucket.geek-guild.net'

class TestS3Access:

    def test_download_one_file(self):
        logger.info('TODO test_download_one_file')

        # case 1
        # download a file in the TEST_BUCKET_NAME bucket to local_root_dir on the local.
        s3_bucket_name = TEST_BUCKET_NAME
        s3_key = 'subdir/train_data.csv'
        local_root_dir = './test/test_data/s3_access/download_one_file/'
        local_file_path = None
        logger.info('case 1:')
        shutil.rmtree(local_root_dir, ignore_errors=True)

        expected_downloaded_file_path = os.path.join(local_root_dir, s3_key)
        s3_access.download(s3_bucket_name, s3_key, local_root_dir, local_file_path)
        TestS3Access._assert_s3_and_local(s3_bucket_name, s3_key, expected_downloaded_file_path)

    def test_download_files(self):

        # case 1
        # download files in the TEST_BUCKET_NAME bucket to local_root_dir on the local.
        s3_bucket_name = TEST_BUCKET_NAME
        s3_key = 'subdir2/'
        local_root_dir = './test/test_data/s3_access/download_files/case1/'
        local_file_path = None
        logger.info('case 1:')
        shutil.rmtree(local_root_dir, ignore_errors=True)
        s3_access.download(s3_bucket_name, s3_key, local_root_dir, local_file_path)

        # case 2
        # download all files in the TEST_BUCKET_NAME bucket to local_root_dir on the local.
        s3_bucket_name = TEST_BUCKET_NAME
        s3_key = None
        local_root_dir = './test/test_data/s3_access/download_files/case2/'
        local_file_path = None
        logger.info('case 2:')
        shutil.rmtree(local_root_dir, ignore_errors=True)
        s3_access.download(s3_bucket_name, s3_key, local_root_dir, local_file_path)
        # TODO assert s3 and local recursively
        logger.info('DONE test_download_one_file')

    def test_upload_one_file(self):
        logger.info('TODO test_upload_one_file')

        # case 1
        # upload a file in local_root_dir on the local to the TEST_BUCKET_NAME bucket.'
        s3_bucket_name = TEST_BUCKET_NAME
        s3_key = None
        local_root_dir = './test/test_data/s3_access/'
        local_file_path = 'to_be_uploaded_dir/upload_one_file/train_data.csv'
        local_file_path_to_be_uploaded = os.path.join(local_root_dir, local_file_path)
        s3_key_to_be_uploaded = local_file_path
        logger.info('case 1: ')
        s3_access.upload(s3_bucket_name, s3_key, local_root_dir, local_file_path)
        TestS3Access._assert_s3_and_local(s3_bucket_name, s3_key_to_be_uploaded, local_file_path_to_be_uploaded)
        logger.info('DONE test_upload_one_file')

    def test_upload_files(self):
        logger.info('TODO test_upload_files')

        # case 1
        # upload a file in local_root_dir on the local to the TEST_BUCKET_NAME bucket.'
        s3_bucket_name = TEST_BUCKET_NAME
        s3_key = None
        local_root_dir = './test/test_data/s3_access/'
        local_file_path = 'to_be_uploaded_dir/upload_files/'
        logger.info('case 1: ')
        s3_access.upload(s3_bucket_name, s3_key, local_root_dir, local_file_path)
        # TODO assert s3 and local recursively
        logger.info('DONE test_upload_files')

    def test_upload_all_files(self):
        logger.info('TODO test_upload_all_files')

        # case 1
        # upload a file in local_root_dir on the local to the TEST_BUCKET_NAME bucket.'
        s3_bucket_name = TEST_BUCKET_NAME
        s3_key = None
        local_root_dir = './test/test_data/s3_access/to_be_uploaded_all_files_dir/'
        local_file_path = None
        logger.info('case 1: ')
        s3_access.upload(s3_bucket_name, s3_key, local_root_dir, local_file_path)
        # TODO assert s3 and local recursively
        logger.info('DONE test_upload_all_files')


    @staticmethod
    def _assert_s3_and_local(s3_bucket_name, s3_key, local_file_path):
        logger.debug('_assert_s3_and_local s3_bucket_name: {}, s3_key: {}, local_file_path:{}'.format(s3_bucket_name, s3_key, local_file_path))

        tmp_dir_path = './tmp/'
        shutil.rmtree(tmp_dir_path, ignore_errors=True)
        os.makedirs(tmp_dir_path, exist_ok=True)

        # download from s3 to tmp_local_path
        tmp_local_path = os.path.join(tmp_dir_path, 'tmp_{}'.format(time.time()))
        s3 = boto3.resource('s3')
        s3.Bucket(s3_bucket_name).download_file(s3_key, tmp_local_path)

        s3_file_checksum = TestS3Access.md5_checksum(tmp_local_path)
        local_file_checksum = TestS3Access.md5_checksum(local_file_path)
        logger.debug('s3_file_checksum: {}, local_file_checksum:{}'.format(s3_file_checksum, local_file_checksum))

        shutil.rmtree('./tmp/', ignore_errors=True)
        assert s3_file_checksum == local_file_checksum

        return False

    @staticmethod
    def md5_checksum(file_path):
        logger.debug('file_path: {}'.format(file_path))
        hash_md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def test_all(self):
        logger.info('TODO test_all')
        self.test_download_one_file()
        self.test_download_files()
        self.test_upload_one_file()
        self.test_upload_files()
        self.test_upload_all_files()
        logger.info('DONE test_all')
