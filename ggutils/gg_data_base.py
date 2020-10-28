# Copyright 2018-2020 Geek Guild Co., Ltd.
# ==============================================================================

import pandas
import pickle
import redis
import hashlib
import json
from ggutils import get_module_logger
log = get_module_logger()

class Singleton:
    """
    Simple Singletons class
      - Non-thread-safe
      - Use with decoration class with @Singleton
      - The @Singleton class cannot be inherited from.
      - The @Singleton class  provides the singleton instance by the `Instance` method.

    """

    def __init__(self, decorated):
        self._decorated = decorated

    def Instance(self):
        """
        Returns the singleton instance.
        """
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)

@Singleton
class GGDataBase:

    def set_db(self, **kwargs):
        default_setting_dict ={
          "host": "localhost",
          "port": 6379,
          "use_redis_cluster": False,
          "redis_db_num": 0,
          "key_hashing": None,
          "use_redis_hash": False,
          "key_delimiter": "/",
          "use_connection_pool": False,
          "ssl": False,
          "password": None
        }

        try:
            with open(kwargs['setting_file_path']) as f:
                setting_dict = json.load(f)
                # log.debug('setting_dict from json:{}'.format(setting_dict))
        except Exception as e:
            log.error(e)
            setting_dict = kwargs

        # check args
        for param, default_value in default_setting_dict.items():
            if not param in setting_dict: setting_dict[param] = default_value

        # log.debug('setting_dict:{}'.format(setting_dict))
        # password connection only with ssl

        self._host = setting_dict['host']
        self._port = setting_dict['port']
        self._redis_db_num = setting_dict['redis_db_num']
        self._key_hashing = setting_dict['key_hashing']
        self._use_redis_hash = setting_dict['use_redis_hash']
        self._key_delimiter = setting_dict['key_delimiter']
        self._use_connection_pool = setting_dict['use_connection_pool']
        log.info('use_redis_cluster:{}'.format(setting_dict['use_redis_cluster']))
        if setting_dict['use_redis_cluster']:
            # check cluster setting
            from rediscluster.exceptions import RedisClusterException
            try:
                from rediscluster import StrictRedisCluster
                self._db_ins = redis.StrictRedisCluster(startup_nodes=setting_dict['startup_nodes'],
                                                        password=setting_dict['password'],
                                                        skip_full_coverage_check=True)  # for RedisCluster
                log.info('StrictRedisCluster is initialized with startup_nodes:{}'.format(setting_dict['startup_nodes']))
                return
            except KeyError as e:
                log.error(e)
            except RedisClusterException as e:
                log.error(e)

        if setting_dict['use_connection_pool'] is not None and setting_dict['use_connection_pool']:
            connection_class = redis.SSLConnection if setting_dict['ssl'] else redis.connection.Connection
            self._connection_pool = redis.ConnectionPool(host=setting_dict['host'], port=setting_dict['port'], db=setting_dict['redis_db_num'], connection_class=connection_class, password=setting_dict['password'])
            self._db_ins = redis.StrictRedis(connection_pool=self._connection_pool, ssl=setting_dict['ssl'])
            log.info('StrictRedis is initialized with ConnectionPool')
        else:
            self._db_ins = redis.StrictRedis(host=setting_dict['host'], port=setting_dict['port'], db=setting_dict['redis_db_num'], ssl=setting_dict['ssl'], password=setting_dict['password'])
            log.info('StrictRedis is initialized')

    def get_host(self):
        return self._host

    def keys(self, pattern=None, group_key=None, has_large_keys=True):
        '''
        :return: all keys
        '''
        log.debug('pattern:{}, group_key:{}'.format(pattern, group_key))
        if group_key is not None:
            if pattern is None:
                if self._use_redis_hash:
                    ret_keys = self._db_ins.hkeys(group_key)
                else:
                    ret_keys = self._db_ins.scan_iter(group_key + self._key_delimiter + '*')
                log.debug('returns ret_keys:{}'.format(ret_keys))
                return list(ret_keys)
            else:
                if self._use_redis_hash:
                    scaned_keys = [tuple[0] for tuple in self._db_ins.hscan_iter(group_key, match=pattern)]
                else:
                    scaned_keys = list(self._db_ins.scan_iter(group_key + self._key_delimiter + pattern))
                log.debug('returns scaned_keys:{}'.format(scaned_keys))
                return scaned_keys

        if pattern is None:
            return list(self._db_ins.scan_iter('*'))
        else:
            return list(self._db_ins.scan_iter(pattern))

    def read(self, key, key_hashing=None):
        key = self.convert_key_with_key_hashing(key, key_hashing)
        read_ojb = self._db_ins.get(key)
        try:
            if read_ojb is None: return None
            return pickle.loads(read_ojb)
        except TypeError as e:
            log.warn('Warning can not picle.loads read_ojb:{}'.format(read_ojb))


    def read_file_to_db(self, file_path, key=None, dt_col_name=None, key_hashing=None):
        if key is None:
            key = file_path

        key = self.convert_key_with_key_hashing(key, key_hashing)

        if dt_col_name is None:
            df = pandas.read_csv(file_path)
        else:
            df = pandas.read_csv(file_path, parse_dates=[dt_col_name], index_col=dt_col_name)

        self.update(key, df, key_hashing)

        return df

    def update(self, key, value, key_hashing=None):
        key = self.convert_key_with_key_hashing(key, key_hashing)
        self._db_ins.set(key, pickle.dumps(value))

    def delete(self, key, key_hashing=None):
        key = self.convert_key_with_key_hashing(key, key_hashing)
        self._db_ins.delete(key)

    def read_range(self, key, range1=None, range2=None, key_hashing=None):
        key = self.convert_key_with_key_hashing(key, key_hashing)
        if range1 is None:
            _range1 = 0
            _range2 = -1
        else:
            if range2 is None:
                _range1 = 0
                _range2 = range1 - 1
            else:
                _range1 = range1
                _range2 = range2 - 1

        values = self._db_ins.lrange(key, _range1, _range2)
        return [pickle.loads(v) for v in values]

    def push(self, key, value, key_hashing=None):
        key = self.convert_key_with_key_hashing(key, key_hashing)
        self._db_ins.rpush(key, pickle.dumps(value))


    def read_with_group_key(self, group_key, key, key_hashing=None):
        group_key = self.convert_key_with_key_hashing(group_key, key_hashing)
        key = self.convert_key_with_key_hashing(key, key_hashing)

        # if already key contain group_key, then remove it
        if key.find(group_key + self._key_delimiter) == 0: key = key[len(group_key + self._key_delimiter):]

        read_ojb = self._db_ins.hget(group_key, key) if self._use_redis_hash else self._db_ins.get(
            group_key + self._key_delimiter + key)
        try:
            if read_ojb is None: return None
            return pickle.loads(read_ojb)
        except TypeError as e:
            log.warn('Warning can not picle.loads read_ojb:{}'.format(read_ojb))


    def update_with_group_key(self, group_key, key, value, key_hashing=None):
        group_key = self.convert_key_with_key_hashing(group_key, key_hashing)
        key = self.convert_key_with_key_hashing(key, key_hashing)

        # if already key contain group_key, then remove it
        if key.find(group_key + self._key_delimiter) == 0: key = key[len(group_key + self._key_delimiter):]

        if self._use_redis_hash:
            self._db_ins.hset(group_key, key, pickle.dumps(value))
        else:
            self._db_ins.set(group_key + self._key_delimiter + key, pickle.dumps(value))

    def delete_with_group_key(self, group_key, key, key_hashing=None):
        group_key = self.convert_key_with_key_hashing(group_key, key_hashing)
        key = self.convert_key_with_key_hashing(key, key_hashing)

        # if already key contain group_key, then remove it
        if key.find(group_key + self._key_delimiter) == 0: key = key[len(group_key + self._key_delimiter):]

        if self._use_redis_hash:
            self._db_ins.hdel(group_key, key)
        else:
            self._db_ins.delete(group_key + self._key_delimiter + key)

    def convert_key_with_key_hashing(self, key, key_hashing):
        if key is None: return key
        if key_hashing is not None: key_hashing=self._key_hashing
        if key_hashing is not None:
            if key_hashing == 'sha256':
                return _generate_sha256_hash(key)
            else:
                raise KeyError('key_hashing is allowed only with sha256')
        else:
            return key

    def save(self):
        self._db_ins.save()


def _generate_sha256_hash(src_str):
    digest = hashlib.sha256(str(src_str).encode()).hexdigest()
    return digest


def test_read_keys():
    setting_file_path = '/usr/local/etc/vendor/gg/redis_connection_setting.json'
    db = GGDataBase.Instance()
    db.set_db(setting_file_path=setting_file_path, debug_mode=False)

    def read_group_key(group_key):
        keys = db.keys(group_key=group_key)
        if keys is not None:
            log.info('========== read with keys:{}'.format(keys))
            for key in keys:
                value = db.read_with_group_key(group_key=group_key, key=key)
                log.info('key:{}, value:{}'.format(key, value))

    group_key = 'test_push_save'
    read_group_key(group_key)

if __name__ == '__main__':
    test_read_keys()

