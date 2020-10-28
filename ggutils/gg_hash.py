# Copyright 2018-2020 Geek Guild Co., Ltd.
# ==============================================================================

from datetime import datetime
import numpy as np
import pandas as pd
import sys
from ggutils.gg_data_base import GGDataBase

from ggutils import get_module_logger
log = get_module_logger()

import json
class ExtendedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        if isinstance(obj, (datetime)):
            return obj.isoformat()
        if isinstance(obj, (np.int32, np.int64)):
            return str(obj)
        if isinstance(obj, (np.float32, np.float64)):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def list_to_hash(arg_list):
    json_str = json.dumps(arg_list, cls=ExtendedJSONEncoder)
    log.debug('json_str of arg_list:{}'.format(json_str))
    import hashlib
    data_hash_value = hashlib.sha256(json_str.encode()).hexdigest()
    log.debug('data_hash_value:{}'.format(data_hash_value))
    return data_hash_value

# cache_db_host = 'localhost'
# use_cache = False
witout_check_cache = False

class GGHash:
    def __init__(self, name, use_db='GGDataBase', setting_file_path='/usr/local/etc/vendor/gg/redis_connection_setting.json', refresh=False, dtype=np.ndarray):
        PREFIX = '[GGHash]'
        if use_db is None: use_db = 'Default'
        self.use_db = use_db
        self.dtype = dtype
        self.name = name
        self.group_key = name
        if use_db == 'GGDataBase':
            self._db = GGDataBase.Instance()
            self._db.set_db(setting_file_path=setting_file_path, debug_mode=False)
            if refresh:
                log.info('refresh with delete group_key:{}'.format(self.group_key))
                for key in self.get_keys():
                    log.info('refresh with delete key:{}'.format(key))
                    self._db.delete(key)
                self._db.delete(self.group_key)

        elif use_db == 'Default':
            # Default simple k-v dictionary
            self._db = {}
        else:
            raise Exception('Invalid self.use_db:{}'.format(self.use_db))

    def set_info(self, **kwargs):
        self.set('info', kwargs)

    def get_info(self):
        return self.get('info')

    def get(self, key=None):
        _iterable, key_or_list = self.is_key_iterable(key)
        if key is None or (_iterable and len(key_or_list) == 0):
            raise Exception('Invalid usage of get with empty key:{}. Use get_all_values.'.format(key))
        else:
            _iterable, key_or_list = self.is_key_iterable(key)
            if self.use_db in ['GGDataBase']:
                if _iterable:
                    return np.asarray([self.cast_dtype(self._db.read_with_group_key(self.group_key, k)) for k in key_or_list])
                else:
                    return self.cast_dtype(self._db.read_with_group_key(self.group_key, key))
            elif self.use_db == 'Default':
                if _iterable:
                    return np.asarray([self._db[k] for k in key_or_list])
                else:
                    return np.asarray(self._db[key])
            raise Exception('Invalid self.use_db:{}'.format(self.use_db))

    def cast_dtype(self, value):
        if isinstance(self.dtype, np.ndarray) and not isinstance(value, np.ndarray): return np.asarray(value)
        if isinstance(self.dtype, pd.DataFrame) and not isinstance(value, pd.DataFrame): return pd.DataFrame(value)
        if isinstance(self.dtype, pd.Series) and not isinstance(value, pd.Series): return pd.Series(value)
        # raise ValueError('invalid dtype:{}'.format(self.dtype))
        # do nothing
        return value


    def set(self, key, value):
        if self.use_db in ['GGDataBase']:
            self._db.update_with_group_key(self.group_key, key, value)
            return
        elif self.use_db == 'Default':
            self._db[key] = value
            return

        raise Exception('Invalid self.use_db:{}'.format(self.use_db))

    def delete(self, key):
        if self.use_db in ['GGDataBase']:
            self._db.delete_with_group_key(self.group_key, key, value)
            return
        elif self.use_db == 'Default':
            if self._db[key]: del self._db[key]
            return

        raise Exception('Invalid self.use_db:{}'.format(self.use_db))


    def get_keys(self, pattern=None):
        if self.use_db in ['GGDataBase']:
            keys = self._db.keys(group_key=self.group_key, pattern=pattern)
            keys = [k.decode('utf-8') if isinstance(k, bytes) else k for k in keys ]
            return keys

        elif self.use_db == 'Default':
            keys = [k for k in self._db.keys() if self.name in k]
            return keys
        raise Exception('Invalid self.use_db:{}'.format(self.use_db))

    def get_all_values(self):
        all_key_with_name = self.get_keys()
        if self.use_db in ['GGDataBase']:
            return np.asarray([self.cast_dtype(self._db.read(k)) for k in all_key_with_name])
        elif self.use_db == 'Default':
            return np.asarray([self._db[k] for k in all_key_with_name])
        raise Exception('Invalid self.use_db:{}'.format(self.use_db))

    def get_size(self):
        if self.use_db in ['Default', 'GGDataBase']:
            keys = self.get_keys()
            return len(keys)
        raise Exception('Invalid self.use_db:{}'.format(self.use_db))

    def shape(self, index=None):

        if index is None:
            # get all shape
            ret_size = [self.get_size()]
            if self.use_db in ['Default', 'GGDataBase']:
                for key in self.get_keys():
                    ret_size.extend(self.get(key).shape[0:]) # TODO only get first key-value's shape
                    return tuple(ret_size)
            raise Exception('Invalid self.use_db:{}'.format(self.use_db))

        elif index == 0:
            return self.get_size()

        elif index > 1:
            if self.use_db in ['Default', 'GGDataBase']:
                for key in self._db.keys():
                    return self.get(key).shape[index-1] # TODO only get first key-value's shape
            raise Exception('Invalid self.use_db:{}'.format(self.use_db))
        else:
            raise Exception('Invalid index:{}'.format(index))

    def is_key_iterable(self, key):
        if key is None: return False, key
        if isinstance(key, list): return True, key
        if isinstance(key, np.ndarray) and len(key) > 1: return True, list(key)
        if isinstance(key, range): return True, list(key)
        return False, key

# base test
# detail test is test/test_gg_hash.py

def test():
    h = GGHash(name='test_gg_hash', refresh=True)
    keys = h.get_keys()
    log.info('keys:{}'.format(keys))
    key_range = range(100, 110)
    for key in key_range:
        value = int(key) * 0.001
        h.set(key, value)
    h.set_info(key_range=key_range)

    log.info('===== after update =====')
    keys = h.get_keys()
    log.info('keys:{}'.format(keys))
    for key in keys:
        value = h.get(key)
        log.info('key:{}, value:{}'.format(key, value))


if __name__ == '__main__':
    test()
