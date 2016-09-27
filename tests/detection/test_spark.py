# (C) Copyright 2016 Guilherme Maluf Balzana <guimalufb at gmail.com>

import mock
import unittest

from monasca_setup.detection.plugins.spark import Spark
import monasca_setup.detection.utils as utils

_SPARK_JAVA_CLASS = 'org.apache.spark.deploy.history.HistoryServer'


class TestSparkDetect(unittest.TestCase):

    def setUp(self):
        super(TestSparkDetect, self).setUp()
        self._spark = Spark('template_dir')
        self._expected_config = {
            'spark': {
                'init_config': None,
                'instances': [{
                    'history_server_url': 'http://localhost:18080/api/v1',
                    'min_date': None
                }]
            }
        }

    def test_no_args_detection(self):
        config = self._spark.build_config()
        self.assertEqual(config, self._expected_config)

    def test_history_server_arg_detection(self):
        _HISTORY_SERVER_URL = 'http://my-history-server:12345/api/v1'
        _MIN_DATE = '2016-10-13T13:00:00'
        self._expected_config = {
            'spark': {
                'init_config': None,
                'instances': [{
                    'history_server_url': _HISTORY_SERVER_URL,
                    'min_date': _MIN_DATE
                }]
            }
        }
        self._spark.args = {
            'history_server_url': _HISTORY_SERVER_URL,
            'min_date': _MIN_DATE,
        }
        config = self._spark.build_config()
        self.assertEqual(config, self._expected_config)

    def test_history_port_detection_arg(self):
        with mock.patch.object(utils, 'find_addr_listening_on_port') as mock_find_port:
            mock_find_port.return_value = True
            self._spark.args = {'history_port': 12345}
            self._spark._detect()

            mock_find_port.assert_called_once_with(12345)

        self.assertTrue(self._spark.available)
