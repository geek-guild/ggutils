from dateutil.parser import parse as parse_datetime
from datetime import timezone
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from datetime import datetime
import time
import pandas as pd
import numpy as np
import math
import csv
import os
from pathlib import Path
import sys
import argparse
import re

# from ggutils import date_util

from ggutils import get_module_logger
log = get_module_logger()

# sys.path.append("../") # for local test
from ggutils.gg_hash import GGHash

class DataProcessor:
    def __init__(self):
        print('TODO')

def _read_csv_with_ckecking(file_path, parse_dates=None, encoding=None):

    try:
        return pd.read_csv(file_path, parse_dates=parse_dates, encoding=encoding)
    except UnicodeDecodeError:
        if encoding is None or encoding == 'utf-8':
            encoding = 'shift_jis'
            return read_csv_with_ckecking(file_path, parse_dates=parse_dates, encoding=encoding)

def read_csv_with_ckecking(file_path, **kwargs):

    try:
        df = pd.read_csv(file_path, **kwargs)
        if df is None: log.info('returns None df, read_csv_with_ckecking file_path:{}'.format(file_path))
        return df
    except UnicodeDecodeError as e:
        if 'encoding' not in kwargs.keys() or kwargs['encoding'] is None or kwargs['encoding'] == 'utf-8':
            kwargs['encoding'] = 'shift_jis'
            return read_csv_with_ckecking(file_path, **kwargs)
        else:
            raise e

def _ckecking(file_path, *args, **kwargs):
    print(args)
    print(kwargs)
    print(locals())
    for k in kwargs:
        v = kwargs[k]
        print('k:{}, v:{}'.format(k, v))

    kwargs['encoding'] = 'shift_jis'

    df = pd.read_csv(file_path, **kwargs)
    print('len of df:{}'.format(len(df)))
    print('df.head:{}'.format(df.head()))
    assert len(df) > 0


def conv_dir(src_data_file_path, dist_data_dir_path, file_path):
    src_data_file_path = str(src_data_file_path)
    dist_data_dir_path = str(dist_data_dir_path)
    file_path = str(file_path)
    try:
        log.info('src_data_file_path:{}, file_path:{}'.format(src_data_file_path, file_path))
        file_path_under_src_data_file_path = file_path.split(src_data_file_path)
        log.info('file_path_under_src_data_file_path:{}'.format(file_path_under_src_data_file_path))
        assert len(file_path_under_src_data_file_path) == 2
        split_file_path = file_path_under_src_data_file_path[1]
        split_file_path = split_file_path.replace('/', '')
        log.info('split_file_path:{}'.format(split_file_path))
        ret_path = os.path.join(dist_data_dir_path, split_file_path)
        log.info('ret_path:{}'.format(ret_path))
        return ret_path
    except ValueError:
        return None


def find_all_files(directory):
    for root, dirs, files in os.walk(directory):
        yield root
        for file in files:
            yield os.path.join(root, file)


def import_csv(src_data_path, data_name, refresh=False, data_col_setting_list=None, keys=None, key_label=None, key_format=None,
             data_col_query_list=None, move_to_save_dir_path=None, thread_retry_interval=5):
    log.info('import_csv src_data_path:{}'.format(src_data_path))

    # set src_data_dir_path from src_data_path
    if os.path.isfile(src_data_path):
        src_data_dir_path = Path(src_data_path).parent
        src_path_list = [src_data_path]
    else:
        src_data_dir_path = src_data_path
        src_path_list = list(find_all_files(src_data_dir_path))
        src_path_list.sort()

    log.info('src_data_dir_path:{}'.format(src_data_dir_path))
    log.info('len(src_path_list):{}'.format(len(src_path_list)))
    assert src_path_list is not None
    assert len(src_path_list) > 0

    hash_data = GGHash(name=data_name, refresh=refresh, dtype=pd.Series)
    log.info('hash_data.name:{}'.format(hash_data.name))
    file_cnt = 0
    all_file_cnt = len(src_path_list)
    for src_path in src_path_list:
        log.info('========== import_csv file_cnt:{} / all_file_cnt:{}'.format(file_cnt, all_file_cnt))
        done_import = False
        file_cnt += 1

        log.info('src_path:{}'.format(src_path))

        if not os.path.isfile(src_path):
            log.info('skip because the path is not a file')
            continue

        df = None
        try:
            df = read_csv_with_ckecking(src_path, header=0)
            if df is None:
                log.info('skip because df read_csv_with_ckecking is None')
                continue
            log.info('----- df read_csv_with_ckecking ----- ')
            log.info(df.head())
            # 1. correct_col_name
            df = correct_col_name(df, data_col_setting_list)
            # 2. select_df_with_query
            df = select_df_with_query(df, data_col_query_list)
            # 3. set_data_col_with_src(also extends list col values)
            df = set_data_col_with_src(df, data_col_setting_list)
            # 4. select col
            df = select_with_data_col_setting_list(df, data_col_setting_list)
            log.info('----- df after correct_col_name ----- ')
            log.info(df.head())
            # 5. set df key cols
            if keys is None:
                if key_format is None:
                    keys = [df.columns[0]]  # default key
                else:
                    keys = get_key_cols_from_key_format(key_format)

            # 6. set key for each data
            df = set_key_with_format(df, key_label=key_label, keys=keys, key_format=key_format)
            log.info('df after set_key_with_format:{}'.format(df.head()))

        except KeyError as e:
            log.error(e)
            continue

        data_size = len(df.index)

        import threading
        import time
        import psutil

        class HashDataThread(threading.Thread):
            def __init__(self, thread_id, key, value=None, name=None, retry_count=3, interval=1,
                         available_cpu_percent=300, available_mem_percent=90,
                         thread_pool=None):
                threading.Thread.__init__(self)
                self.thread_id = thread_id
                self.name = name
                self.retry_count = retry_count
                self.interval = interval
                self.key = key
                self.value = value
                self.available_cpu_percent = available_cpu_percent
                self.available_mem_percent = available_mem_percent
                self.thread_pool = thread_pool


                if self.thread_pool is not None:
                    self.thread_pool.set_activity(thread_id=self.thread_id, activity=True)

            def run(self):
                if self.thread_id % 1000 == 0: log.debug("Starting thread_id:{}".format(self.thread_id))
                self.set_hash_data(self.key, self.value)
                if self.thread_id % 1000 == 0: log.debug("Exiting thread_id:{}".format(self.thread_id))
                if self.thread_pool is not None:
                    self.thread_pool.set_activity(self.thread_id, activity=False)

            def set_hash_data(self, key, value, check_exist=False):
                if value is None: raise ValueError('value is None')
                if check_exist:
                    value_exist = hash_data.get(key)
                    if value_exist is not None:
                        # log.info('skip importing thread_id:{}, key:{}'.format(self.thread_id, data_size, key))
                        return
                interval = self.interval
                retry_count = self.retry_count
                while retry_count >= 0:
                    try:
                        hash_data.set(key, value)
                        return
                    except Exception as e:
                        log.error('TODO retry set for retry_count:{} with error:{}'.format(retry_count, e))
                        # check error message
                        if 'Redis Cluster cannot be connected' in str(e):
                            interval = min(600, interval * 2)
                            retry_count = self.retry_count
                            log.error('Increse interval:{}, retry_count:{}'.format(interval, retry_count))
                        else:
                            interval = self.interval

                    time.sleep(interval)
                    retry_count -= 1
                raise RuntimeError('Error set with thread_id:{}'.format(self.thread_id))

        class ThreadPool(object):
            def __init__(self, max_threads):
                self.active_threads = []
                self.max_threads = max_threads
                log.info('ThreadPool init')

            def set_activity(self, thread_id, activity):
                if activity:
                    self.active_threads.append(thread_id)
                    if self.get_active_thread_number() == self.max_threads:
                        log.debug('ThreadPool get_active_thread_number reaches to self.max_threads:{}'.format(self.max_threads))
                else:
                    self.active_threads.remove(thread_id)

            def get_active_thread_number(self):
                return len(self.active_threads)

            def get_active_threads(self):
                return self.active_threads


        has_set_error = False

        def check_sys_resource(thread_id, available_cpu_percent=100, available_mem_percent=50):
            sys_cpu_percent = psutil.cpu_percent()
            sys_mem_percent = psutil.virtual_memory()._asdict()['percent']
            checked = (
                    sys_cpu_percent < available_cpu_percent and sys_mem_percent < available_mem_percent)
            if thread_id % 1000 == 0:
                log.info('sys_cpu_percent:{}, sys_mem_percent:{}'.format(sys_cpu_percent, sys_mem_percent))
            if not checked:
                log.info('check not OK, sys_cpu_percent:{}, sys_mem_percent:{}'.format(sys_cpu_percent,
                                                                                       sys_mem_percent))
            return checked

        def wait_until_sys_resource_ok(thread_id, available_cpu_percent=100, available_mem_percent=50, wait_interval=0.01):

            while not check_sys_resource(thread_id):
                wait_interval = min(600, wait_interval * 2.0)
                if wait_interval > 1:
                    log.error(
                        'Wait init thread_id:{} for check_sys_resource become OK. wait_interval:{}'.format(
                            thread_id, wait_interval))
                time.sleep(wait_interval)

        max_threads = 20
        thread_pool = ThreadPool(max_threads=max_threads)

        def wait_until_threads(thread_id, wait_interval=0.01, max_threads=100):

            while thread_pool.get_active_thread_number() > max_threads:
                wait_interval = min(600, wait_interval * 2.0)
                if wait_interval > 1:
                    log.error(
                        'Wait init thread_id:{} for active_thread_number get lower than max_threads:{}. wait_interval:{}'.format(
                            thread_id, max_threads, wait_interval))
                time.sleep(wait_interval)

        last_time = time.time()
        for i, key in enumerate(df.index):
            retry_count = 3
            while retry_count > 0:
                try:
                    # wait_until_sys_resource_ok(thread_id=i)
                    wait_until_threads(thread_id=i, max_threads=max_threads)
                    th = HashDataThread(thread_id=i, key=key, value=df.loc[key], thread_pool=thread_pool)
                    th.start()
                    if i % 1000 == 0:
                        log.info('importing thread_id:{}, key:{}, time:{}'.format(i, key,
                                                                                  time.time() - last_time))
                        last_time = time.time()

                    break
                except RuntimeError as e:
                    log.error('thread_id:{}, error:{}'.format(i, e))


                retry_count -= 1
                time.sleep(thread_retry_interval)
            if retry_count == 0:
                # raise RuntimeError('can not start new thread for 3 times')
                has_set_error = True
                break
        done_import = (not has_set_error)
        if not done_import:
            log.error('Failed to import:{}. Try it again.'.format(src_path))
            continue
        #
        if done_import and move_to_save_dir_path is not None:
            os.makedirs(move_to_save_dir_path, exist_ok=True)
            move_to_save_file_path = os.path.join(move_to_save_dir_path, os.path.basename(src_path))
            os.rename(src_path, move_to_save_file_path)

    return hash_data

def set_key_with_format(df, key_label=None, keys=None, key_format=None, default_key_delimiter='-'):
    generated_key_list = generate_key_with_format(df, keys, key_format, default_key_delimiter)

    if key_label is None: key_label = 'key'
    df[key_label] = generated_key_list
    df = df.set_index(key_label)
    return df

def generate_key_with_format(df, keys=None, key_format=None, default_key_delimiter='-'):
    if keys is None:
        keys = [df.columns[0]]  # default key
    # keys = ['key1', 'key2']
    if key_format is None:
        key_format = default_key_delimiter.join(['{' + str(s) + '}' for s in keys])
        # values = [10, 20]
    log.info('keys:{}, key_format:{}'.format(keys, key_format))
    generated_key_list = []
    log.info('df[keys]:{}'.format(df[keys]))
    key_values_list = df[keys].values
    log.info('key_values_list:{}'.format(key_values_list))
    for i, key_values in enumerate(key_values_list):
        format_dict = get_format_dict(keys, key_values)
        generated_key = key_format.format(**format_dict)
        if i == 0:
            log.info('generated_key:{}'.format(generated_key))
        generated_key_list.append(generated_key)

    return generated_key_list

def get_key_cols_from_key_format(key_format):
    log.info('key_format:{}'.format(key_format))
    key_cols = re.findall(r"\{([0-9a-z\_]+)\}", key_format)
    log.info('key_cols:{}'.format(key_cols))
    return key_cols

def get_format_dict(keys, values):
    format_dict = {}
    for i, key in enumerate(keys):
        format_dict[key] = values[i]
    return format_dict


def get_columns(data_col_setting_list):
    _columns = []
    for data_col_setting in data_col_setting_list:
        if 'col_name' in data_col_setting.keys():
            _columns.append(data_col_setting['col_name'])
    return _columns

def select_with_data_col_setting_list(df, data_col_setting_list):
    _columns = get_columns(data_col_setting_list)
    # select _columns from df columns
    columns_to_select = [col for col in df.columns if col in _columns]
    return df[columns_to_select]

def correct_col_name(df, data_col_setting_list):
    for data_col_setting in data_col_setting_list:
        if 'col_name' in data_col_setting.keys()\
                and ('alias_names' in data_col_setting.keys() or 'alias_names_regex' in data_col_setting.keys()) :
            correct_col_name = data_col_setting['col_name']
            # skip if col_name already exists in df.columns
            if correct_col_name in df.columns: continue

            if 'alias_names' in data_col_setting.keys():
                for alias_name in data_col_setting['alias_names']:
                    if alias_name in df.columns:
                        df = df.rename(columns={alias_name: correct_col_name})
            if 'alias_names_regex' in data_col_setting.keys():
                for alias_names_regex in data_col_setting['alias_names_regex']:
                    for col in df.columns:
                        if col != correct_col_name and re.match(alias_names_regex, col):
                            df = df.rename(columns={col: correct_col_name})

    return df

def select_df_with_query(df, data_col_query_list):
    log.info('data_col_query_list:{}'.format(data_col_query_list))
    if data_col_query_list is not None:
        df_size_before_query = len(df)
        for data_col_query in data_col_query_list:
            log.info('data_col_query:{}'.format(data_col_query))
            try:
                target_data_col = data_col_query.split(' ')[0]
                query_simbol = data_col_query.split(' ')[1]
                query_value = str(data_col_query.split(' ')[2])
                if query_simbol == 'eq':
                    df = df[df[target_data_col].astype(str) == query_value]
                elif query_simbol == 'lt':
                    df = df[df[target_data_col].astype(str) <= query_value]
                elif query_simbol == 'gt':
                    df = df[df[target_data_col].astype(str) >= query_value]

            except ValueError as e:
                log.info('can not execute data_col_query:{} with error:{}'.format(data_col_query, e))
        df_size_after_query = len(df)
        log.info('df_size_before_query:{} df_size_after_query:{}'.format(df_size_before_query, df_size_after_query))

    return df

def set_data_col_with_src(df, data_col_setting_list):
    # part 1. set cols without extending list cols
    for data_col_setting in data_col_setting_list:
        if 'col_name' in data_col_setting.keys() and 'src' in data_col_setting.keys() :
            col_name_to_set = data_col_setting['col_name']
            src = data_col_setting['src']
            # check src setting
            try:
                query, src_col_or_value = src.split(' ')
                log.info('part 1. col_name_to_set:{}, query:{}, src_col_or_value:{}'.format(col_name_to_set, query, src_col_or_value))
                if query == 'const':
                    df[col_name_to_set] = [src_col_or_value] * len(df)
            except ValueError as e:
                continue
    # part 2. set cols with extending list cols
    for data_col_setting in data_col_setting_list:
        if 'col_name' in data_col_setting.keys() and 'src' in data_col_setting.keys() :
            col_name_to_set = data_col_setting['col_name']
            src = data_col_setting['src']
            # check src setting
            try:
                query, src_col_pattern = src.split(' ')
                log.debug('part 2. col_name_to_set:{}, query:{}, src_col_pattern:{}'.format(col_name_to_set, query, src_col_pattern))
                if query == 'list':
                    conved_df = None

                    import re
                    src_col_list = [col_name for col_name in df.columns if re.match(src_col_pattern, col_name)]
                    log.info('src_col_list:{}'.format(src_col_list))
                    if len(src_col_list) == 0:
                        # No need to extend list cols
                        return df
                    
                    base_col_list = [col for col in df.columns if col not in src_col_list]
                    # log.info('base_col_list:{}'.format(base_col_list))

                    df_cols = df.columns

                    for i in df.index:
                        series = df.loc[i]
                        src_col_values = []
                        src_col_values_org = np.asarray([series[src_col] for src_col in src_col_list if series[src_col] is not None])
                        src_col_values_org = [series[src_col] for src_col in src_col_list if series[src_col] is not None]
                        # src_col_values = [src_col_value for src_col_value in src_col_values if not np.isnan(src_col_value)]
                        for j, val in enumerate(src_col_values_org):
                            try:
                                # nan to value or skip nan value
                                if np.isnan(val):
                                    log.debug('skip nan value with j:{} th value of src_col:{}'.format(j, src_col_list[j]))
                                    continue
                            except TypeError as e:
                                log.debug('Warning, skip isnan check for j:{} th value :{} of src_col:{} with error:{}'.format(j, val, src_col_list[j], e))

                            try:
                                if data_col_setting['dtype']:
                                    _to_append_val = np.asarray(val, dtype=data_col_setting['dtype'])
                                else:
                                    _to_append_val = np.asarray(val)
                                log.debug('dtype:{}, from val:{}, _to_append_val:{}'.format(data_col_setting['dtype'], val, _to_append_val))

                                # if 'int' in data_col_setting['dtype'] or 'float' in data_col_setting['dtype']:
                                #     _to_append_val = np.asarray(float(src_col_values_org[i])).astype(data_col_setting['dtype'])

                                src_col_values.append(_to_append_val)
                            except Exception as e:
                                log.error('Can not convert value:{} to dtype:{} with error:{}'.format(val, data_col_setting['dtype'], e))
                                continue
                        if len(src_col_values) == 0: continue

                        np_base_T = np.zeros([len(base_col_list), len(src_col_values)])
                        # log.info('src_col_values:{}, np_base_T:{}'.format(src_col_values, np_base_T))
                        np_to_append = np.vstack((np_base_T, src_col_values)).T
                        df_to_append_columns = base_col_list.copy()
                        df_to_append_columns.append(col_name_to_set)
                        df_to_append = pd.DataFrame(np_to_append, columns=df_to_append_columns)
                        df_to_append = df_to_append.astype({col_name_to_set: data_col_setting['dtype']})
                        # log.info('series:{}'.format(series))
                        # log.info('base_col_list:{}'.format(base_col_list))
                        for base_col in base_col_list:
                            value_to_set = series[base_col]
                            df_to_append[base_col] = value_to_set

                        if conved_df is None:
                            conved_df = df_to_append
                        else:
                            conved_df = pd.concat([conved_df, df_to_append])

                    # update df
                    log.debug('conved_df:{}'.format(conved_df))

                    if conved_df is not None:
                        df = conved_df.copy()

            except ValueError as e:
                log.info('During part 2. set cols with extending list cols, error occuers:{}'.format(e))
                continue


    return df


def set_index_to_hash_data(data_name, key_format=None, indexing=None, refresh=False):
    hash_data = GGHash(name=data_name)
    log.info('hash_data:{}'.format(hash_data))

    log.info('----- select all keys -----')
    all_keys = hash_data.get_keys()
    log.info('len of all_keys:{}'.format(len(all_keys)))

    log.info('----- make_index -----')

    def get_key_col_index_with_key_format(key_format, key_col_name):
        '''
        returns key_col_index of key_col_name witch found in key_format
        :param key_format: e.g. {key_a}-{key_b}
        :param key_col_name:  e.g. key_b, invalid_key
        :return: key_col_index: e.g. 1, None
        '''
        _key_format = key_format.replace('{', '')
        _key_format = _key_format.replace('}', '')

        keys_in_format = _key_format.split('-')
        for i, key_in_format in enumerate(keys_in_format):
            if key_in_format == key_col_name:
                return i

        return None

    def set_index_to_hash_data_with_key_col(index_data_name, all_keys, key_col_name=None, key_col_index=None, refresh=False):
        log.info('----- make_index with key_col_name:{} -----'.format(key_col_name))
        index_hash_data = GGHash(name=index_data_name, refresh=refresh)
        index_key_dict = {}

        if key_col_name is None or key_col_index is None:
            index_key_value = 'all_keys'
            index_key_dict[index_key_value] = all_keys
        # TODO set index with condition of each key value
        else:
            for i, key in enumerate(all_keys):
                # log.info('key:{}'.format(key))
                index_key_value = key.split('-')[key_col_index]
                if i % 1000 == 0: log.info('index_key_value:{}, key:{}'.format(index_key_value, key))
                if index_key_dict.get(index_key_value) is None:
                    index_key_dict[index_key_value] = [str(key)]
                else:
                    index_key_dict[index_key_value].append(str(key))

        log.info('index_key_value:{}'.format(index_key_value))

        from time import sleep
        # set hash
        index_keys = index_key_dict.keys()
        log.info('key_col_name:{}, index_keys:{}'.format(key_col_name, index_keys))
        log.info('TODO set index_hash_data with len of index_key:{}'.format(len(index_keys)))
        for i, index_key in enumerate(index_keys):
            if index_key_dict.get(index_key) is None:
                raise ValueError('index_key_dict of index_key:{} is None. Can not set to index_hash_data'.format(index_key))
            if i % 1000 == 0: log.info(
                'DOING set index_hash_data with i:{} / len of index_key:{}'.format(i, len(index_keys)))

            index_hash_data.set(key=index_key, value=index_key_dict[index_key])

            # check set

        log.info('DONE set index_hash_data with len of index_key:{}'.format(len(index_keys)))
        return

    if indexing is None:
        index_data_name = 'index_{}'.format(data_name)
        set_index_to_hash_data_with_key_col(index_data_name, all_keys, key_col_name=None, refresh=refresh)
    else:
        # TODO
        for key_col_name in indexing:
            key_col_index = get_key_col_index_with_key_format(key_format, key_col_name)
            index_data_name = 'index_{}_{}'.format(data_name, key_col_name)
            log.info('----- make_index with key_col_name:{} -----'.format(key_col_name))
            set_index_to_hash_data_with_key_col(index_data_name, all_keys, key_col_name=key_col_name, key_col_index=key_col_index, refresh=refresh)
    return



def conv_csv(src_data_path, dist_data_dir_path=None, ts_col_name=None, conv_ts_col=True, ts_col_format=None,
             data_col_name_list=None, data_col_query_list=None):
    log.info('src_data_path:{}'.format(src_data_path))

    # set src_data_dir_path from src_data_path
    if os.path.isfile(src_data_path):
        src_data_dir_path = Path(src_data_path).parent
        src_path_list = [src_data_path]
    else:
        src_data_dir_path = src_data_path
        src_path_list = list(find_all_files(src_data_dir_path))
        src_path_list.sort()

    log.info('src_data_dir_path:{}'.format(src_data_dir_path))
    log.info('len(src_path_list):{}'.format(len(src_path_list)))
    assert src_path_list is not None
    assert len(src_path_list) > 0

    conv_out_col_list = []
    if data_col_name_list is None:
        log.info('conv all data col')
        conv_out_col_list = None
    else:
        if ts_col_name is not None:
            # TODO set automatically first col name to ts_col_name
            conv_out_col_list = [ts_col_name]
            conv_out_col_list.extend(data_col_name_list)
        elif data_col_name_list is not None:
            conv_out_col_list = data_col_name_list
        else:
            conv_out_col_list = None

    file_cnt = 0
    all_file_cnt = len(src_path_list)
    for src_path in src_path_list:
        log.info('========== conv_csv file_cnt:{} / all_file_cnt:{}'.format(file_cnt, all_file_cnt))
        file_cnt += 1

        log.info('src_path:{}'.format(src_path))
        dist_path = src_path
        if dist_data_dir_path is not None:
            dist_path = conv_dir(src_data_dir_path, dist_data_dir_path, src_path)
            os.makedirs(Path(dist_path).parent, exist_ok=True)
        log.info('dist_path:{}'.format(dist_path))

        if not os.path.isfile(src_path):
            log.info('skip because the path is not a file')
            continue

        df = None
        try:
            if conv_out_col_list is None:
                df = read_csv_with_ckecking(src_path, header=0)
            else:
                try:
                    df = read_csv_with_ckecking(src_path, header=0, usecols=conv_out_col_list)
                except ValueError as e:
                    log.info('no usecols to read csv because of ValueError:{}'.format(e))
                    df = read_csv_with_ckecking(src_path, header=0)

        except KeyError as e:
            log.info('skip because of KeyError:{}'.format(e))
            continue

        if df is None:
            log.info('skip because the path is not a data file')
            continue

        if len(df) == 0:
            log.info('skip because the path is not a data file')
            continue
        log.info('df.head:{}'.format(df.head()))

        log.info('data_col_query_list:{}'.format(data_col_query_list))
        if data_col_query_list is not None:
            df_size_before_query = len(df)
            for data_col_query in data_col_query_list:
                log.info('data_col_query:{}'.format(data_col_query))
                try:
                    target_data_col = data_col_query.split(' ')[0]
                    query_simbol = data_col_query.split(' ')[1]
                    query_value = str(data_col_query.split(' ')[2])
                    if query_simbol == 'eq':
                        df = df[df[target_data_col].astype(str) == query_value]
                    elif query_simbol == 'lt':
                        df = df[df[target_data_col].astype(str) <= query_value]
                    elif query_simbol == 'gt':
                        df = df[df[target_data_col].astype(str) >= query_value]

                except ValueError as e:
                    log.info('can not execute data_col_query:{} with error:{}'.format(data_col_query, e))
            df_size_after_query = len(df)
            log.info('df_size_before_query:{} df_size_after_query:{}'.format(df_size_before_query, df_size_after_query))

        if ts_col_name is not None and conv_ts_col:
            # drop
            log.info('len(df) before drop:{}'.format(len(df)))
            df.dropna(subset=[ts_col_name])

            _ts = df[ts_col_name].astype(str)
            df[ts_col_name] = [parse_datetime_with_conv(str_dt, dt_format=ts_col_format, invalid_dt_replacer=np.nan) for
                               str_dt in _ts]

            # drop again with invalid_dt_replacer=np.nan
            log.info('len(df) before drop with invalid_dt_replacer:{}'.format(len(df)))
            df.dropna(subset=[ts_col_name])

        log.info('len(df) to_csv:{}'.format(len(df)))
        df.to_csv(dist_path, sep=',', index=False, encoding='utf-8')

    return


def parse_datetime_with_conv(str_dt, dt_format=None, invalid_dt_replacer=np.nan):
    try:
        if dt_format is None:
            return parse_datetime(str_dt)
        elif dt_format == '%D.%M.%Y':
            return datetime(int(str_dt[6:10]), int(str_dt[3:5]), int(str_dt[0:2]))
        else:
            try:
                return datetime.strptime(str_dt, dt_format)
            except:
                return parse_datetime(str_dt)
    except:
        return invalid_dt_replacer

    return invalid_dt_replacer

def get_date_str_from_file_path(file_path, dt_format=None):
    try:
        if dt_format is None: dt_format = '%Y%M%D'
        if dt_format == '%D.%M.%Y':
            return re.findall(r"[0-9]{2}\.[0-9]{2}\.[0-9]{4}", file_path)[0]
        elif dt_format == '%y%M%D':
            str_date = "20" + re.findall(r"[0-9]{6}", file_path)[0]
            assert parse_datetime_with_conv(str_date) > datetime(1999, 3, 31) # check datetime
            return str_date
        else:
            return re.findall(r"[0-9]{8}", file_path)[0]
    except Exception as e:
        log.error(e)
        return None

def info(df):
    import io
    buffer = io.StringIO()
    df.info(buf=buffer)
    return buffer.getvalue()


