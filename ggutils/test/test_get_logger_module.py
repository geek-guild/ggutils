# usage
# cd $REPOSITORY_PARENT_DIR/ggutils/ggutils/
# pytest -v test/test_get_logger_module.py -k "test_all"

from ggutils import get_logger_module
import re

class TestGetLoggerModule:

    def test_info_log(self):
        print('TODO test_info_log')
        logger = get_logger_module()

        # case 1
        message = 'This is info log.'
        logger.info(message)
        log_file_path = 'log/ggutils.log'
        regex_to_match = message
        assert TestGetLoggerModule._assert_log_file(log_file_path, regex_to_match)
        print('DONE test_info_log')


    @staticmethod
    def _assert_log_file(log_file_path, regex_to_match):

        print('_assert_log_file find_pattern_list: {}, regex_to_match:{}'.format(log_file_path, regex_to_match))
        log_message_list = []
        with open(log_file_path) as f:
            log_message_list = f.read().splitlines()
            f.close()
        assert len(log_message_list) > 0

        for log_message in log_message_list:
            print('log_message: {}'.format(log_message))
            find_pattern_list = re.findall(regex_to_match, log_message)
            if len(find_pattern_list) > 0:
                print('_assert_log_file found find_pattern_list: {}, regex_to_match:{}'.format(find_pattern_list, regex_to_match))
                return True
        print('_assert_log_file not found regex_to_match:{}'.format(regex_to_match))
        return False


    def test_all(self):
        print('TODO test_all')
        self.test_info_log()
        print('DONE test_all')
