# (C) Copyright 2016 Guilherme Maluf Balzana <guimalufb at gmail.com>

import datetime
import os
import json
import mock
import requests
import unittest

from monasca_agent.common import util
from monasca_agent.collector.checks_d.spark import Spark

_SPARK_HISTORY_SERVER = 'http://spark:18080/api/v1'


class MockSparkCheck(Spark):
    def __init__(self):
        super(MockSparkCheck, self).__init__(
            name='spark',
            init_config={},
            instances=[{
                'history_server_url': _SPARK_HISTORY_SERVER,
                'min_date': '2015-12-31'
            }],
            agent_config={},
        )


class SparkCheckTest(unittest.TestCase):
    def setUp(self):
        super(SparkCheckTest, self).setUp()
        fixture_file = os.path.dirname(os.path.abspath(__file__)) + '/fixtures/test_spark.json'
        self.fixture = json.load(file(fixture_file))

        self.dimensions = {'hostname': 'test',
                           'component': 'spark_history',
                           'service': 'spark',
                           'history_server_url': _SPARK_HISTORY_SERVER}

        with mock.patch.object(util, 'get_hostname'):
            self.spark = MockSparkCheck()

    def test_instace_presence(self):
        """Should fail when no instance is defined"""
        with self.assertRaises(Exception) as err:
            self.spark.instances = []
            self.spark.check({})
        self.assertEqual('At least one instance should be defined',
                         err.exception.message)

    def test_history_server_url_address(self):
        """Should fail without history_server_url address"""
        with self.assertRaises(Exception) as err:
            self.spark.instances = [{'name': 'error_instance'}]
            self.spark.check(self.spark.instances[0])
        self.assertEqual('A Spark history_server_url must be specified',
                         err.exception.message)

    def test_get_api_request_errors(self):
        """Should emit exception on connections errors and return empty dict"""

        with mock.patch('requests.get') as mock_get:
            mock_response = mock.Mock()
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
            mock_response.ok = False

            mock_get.return_value = mock_response

            return_value = self.spark._get_api_request(url=_SPARK_HISTORY_SERVER)

            mock_get.assert_called_once_with(url=_SPARK_HISTORY_SERVER,
                                             params={})

        self.assertEqual(1, mock_response.raise_for_status.call_count)
        self.assertEqual(0, mock_response.json.call_count)
        self.assertEqual({}, return_value)

    def test_get_applications(self):
        """Should return a list of applications"""

        with mock.patch.object(self.spark, '_get_api_request') as mock_get_api:
            mock_get_api.return_value = self.fixture['applications']

            json = self.spark._get_applications(url=_SPARK_HISTORY_SERVER)
            mock_get_api.assert_called_once_with(url=_SPARK_HISTORY_SERVER, path='/applications',
                                                 params={'status': None,
                                                         'minDate': None,
                                                         'maxDate': None})
        self.assertIsNotNone(json)
        self.assertTrue('id' in json[0])
        self.assertTrue('name' in json[0])
        self.assertTrue('attempts' in json[0])

    def test_get_applications_with_params(self):
        """Should filter request with params"""
        with mock.patch.object(self.spark, '_get_api_request') as mock_get_api:
            self.spark._get_applications(url=_SPARK_HISTORY_SERVER,
                                         status='completed',
                                         minDate='2015-11-20',
                                         maxDate='2015-12-20')

            mock_get_api.assert_called_once_with(url=_SPARK_HISTORY_SERVER, path='/applications',
                                                 params={'status': 'completed',
                                                         'minDate': '2015-11-20',
                                                         'maxDate': '2015-12-20'})

    def test_get_jobs(self):
        with mock.patch.object(self.spark, '_get_api_request') as mock_get_api:
            application = self.fixture['applications'][0]['id']
            mock_get_api.return_value = self.fixture['jobs']

            json = self.spark._get_jobs(url=_SPARK_HISTORY_SERVER, application_id=application)
            mock_get_api.assert_called_once_with(url=_SPARK_HISTORY_SERVER,
                                                 path=('/applications/%s/jobs' % application),
                                                 params={'status': None})

        self.assertIsNotNone(json)
        self.assertIn('jobId', json[0])

    def test_get_stages(self):
        with mock.patch.object(self.spark, '_get_api_request') as mock_get_api:
            application = self.fixture['applications'][0]['id']
            mock_get_api.return_value = self.fixture['stages']

            json = self.spark._get_stages(url=_SPARK_HISTORY_SERVER, application_id=application)
            mock_get_api.assert_called_once_with(url=_SPARK_HISTORY_SERVER,
                                                 path=('/applications/%s/stages' % application))
        self.assertIsNotNone(json)
        self.assertIn('stageId', json[0])

    def test_get_stage(self):
        with mock.patch.object(self.spark, '_get_api_request') as mock_get_api:
            application = self.fixture['applications'][0]['id']
            stage = 0

            self.spark._get_stage(url=_SPARK_HISTORY_SERVER, application_id=application,
                                  stage_id=stage)
            mock_get_api.assert_called_once_with(url=_SPARK_HISTORY_SERVER,
                                                 path=('/applications/%s/stages/%s' % (application,
                                                                                       str(stage))),
                                                 params={'status': None})

    def test_get_executors(self):
        with mock.patch.object(self.spark, '_get_api_request') as mock_get_api:
            application = self.fixture['applications'][0]['id']
            mock_get_api.return_value = self.fixture['executors']

            json = self.spark._get_executors(url=_SPARK_HISTORY_SERVER, application_id=application)
            mock_get_api.assert_called_once_with(url=_SPARK_HISTORY_SERVER,
                                                 path=('/applications/%s/executors' % application))
        self.assertIsNotNone(json)
        self.assertIn('executorLogs', json[0])

    def test_get_storages(self):
        with mock.patch.object(self.spark, '_get_api_request') as mock_get_api:
            application = self.fixture['applications'][0]['id']
            mock_get_api.return_value = self.fixture['storages']

            json = self.spark._get_storages(url=_SPARK_HISTORY_SERVER, application_id=application)
            mock_get_api.assert_called_once_with(url=_SPARK_HISTORY_SERVER,
                                                 path=('/applications/%s/storage/rdd' %
                                                       application))
        self.assertIsNotNone(json)
        self.assertIn('diskUsed', json[0])

    def test_get_storage(self):
        with mock.patch.object(self.spark, '_get_api_request') as mock_get_api:
            application = self.fixture['applications'][0]['id']
            mock_get_api.return_value = self.fixture['storages']
            storage = 0

            json = self.spark._get_storage(url=_SPARK_HISTORY_SERVER, application_id=application,
                                           rdd_id=storage)
            mock_get_api.assert_called_once_with(url=_SPARK_HISTORY_SERVER,
                                                 path=('/applications/%s/storage/rdd/%s' %
                                                       (application, storage)))
        self.assertIsNotNone(json)
        self.assertIn('diskUsed', json[0])

    def test_fetch_and_prepare_data(self):
        with mock.patch.object(self.spark, '_get_jobs') as mock_get_jobs, \
             mock.patch.object(self.spark, '_get_stages') as mock_get_stages, \
             mock.patch.object(self.spark, '_get_storages') as mock_get_storages, \
             mock.patch.object(self.spark, '_get_executors') as mock_get_executors, \
             mock.patch.object(self.spark, '_get_applications') as mock_get_applications:
            mock_get_jobs.return_value = self.fixture['jobs']
            mock_get_stages.return_value = self.fixture['stages']
            mock_get_storages.return_value = self.fixture['storages']
            mock_get_executors.return_value = self.fixture['executors']
            mock_get_applications.return_value = self.fixture['applications']
            orig_app_id = self.fixture['applications'][0]['id']
            orig_app_name = self.fixture['applications'][0]['name']

            results = self.spark._fetch_data(_SPARK_HISTORY_SERVER)
        self.assertIn('jobs', results[0])
        self.assertIn('stages', results[0])
        self.assertIn('storages', results[0])
        self.assertIn('executors', results[0])
        self.assertEqual(results[0]['id'], orig_app_id)
        self.assertEqual(results[0]['name'], orig_app_name)

    def test_check_freq_precedence(self):
        """InstanceConfig check_freq should have precedence over AgentConfig"""
        self.spark.init_config['check_freq'] = 15
        self.spark.agent_config['check_freq'] = 30
        check_freq = self.spark.init_config.get('check_freq',
                                                self.spark.agent_config.get('check_freq', 0))
        self.assertEqual(check_freq, 15)

        self.spark.init_config = {}
        self.spark.agent_config['check_freq'] = 30
        check_freq = self.spark.init_config.get('check_freq',
                                                self.spark.agent_config.get('check_freq', 0))
        self.assertEqual(check_freq, 30)

        self.spark.init_config = {}
        self.spark.agent_config = {}
        check_freq = self.spark.init_config.get('check_freq',
                                                self.spark.agent_config.get('check_freq', 0))
        self.assertEqual(check_freq, 0)

    def test_collect_metrics(self):
        METRICS = [
            'spark.application.duration',
            'spark.application.executor.active_tasks',
            'spark.application.executor.completed_tasks',
            'spark.application.executor.disk_used',
            'spark.application.executor.failed_tasks',
            'spark.application.executor.max_memory',
            'spark.application.executor.memory_used',
            'spark.application.executor.memory_used',
            'spark.application.executor.rdd_blocks',
            'spark.application.executor.total_duration',
            'spark.application.executor.total_input_bytes',
            'spark.application.executor.total_shuffle_read',
            'spark.application.executor.total_shuffle_write',
            'spark.application.executor.total_tasks',
            'spark.application.job.duration',
            'spark.application.job.num_active_stages',
            'spark.application.job.num_active_tasks',
            'spark.application.job.num_completed_stages',
            'spark.application.job.num_completed_tasks',
            'spark.application.job.num_failed_stages',
            'spark.application.job.num_failed_tasks',
            'spark.application.job.num_skipped_stages',
            'spark.application.job.num_skipped_tasks',
            'spark.application.job.num_tasks',
            'spark.application.job.stage.disk_bytes_spilled',
            'spark.application.job.stage.executor_run_time',
            'spark.application.job.stage.input_bytes',
            'spark.application.job.stage.input_records',
            'spark.application.job.stage.memory_bytes_spilled',
            'spark.application.job.stage.num_active_tasks',
            'spark.application.job.stage.num_complete_tasks',
            'spark.application.job.stage.num_failed_tasks',
            'spark.application.job.stage.output_bytes',
            'spark.application.job.stage.output_records',
            'spark.application.job.stage.shuffle_read_bytes',
            'spark.application.job.stage.shuffle_read_records',
            'spark.application.job.stage.shuffle_write_bytes',
            'spark.application.job.stage.shuffle_write_records',
            'spark.application.storage.disk_used',
            'spark.application.storage.memory_used',
            'spark.application.storage.num_cached_partitions',
            'spark.application.storage.num_partitions',
        ]

        with mock.patch.object(self.spark, '_get_jobs') as mock_get_jobs, \
            mock.patch.object(self.spark, '_get_stages') as mock_get_stages, \
            mock.patch.object(self.spark, '_get_storages') as mock_get_storages, \
            mock.patch.object(self.spark, '_get_executors') as mock_get_executors, \
            mock.patch.object(self.spark, '_get_applications') as mock_get_applications:
            mock_get_jobs.return_value = self.fixture['jobs']
            mock_get_stages.return_value = self.fixture['stages']
            mock_get_storages.return_value = self.fixture['storages']
            mock_get_executors.return_value = self.fixture['executors']
            mock_get_applications.return_value = self.fixture['applications']

            data = self.spark._fetch_data(_SPARK_HISTORY_SERVER)
            self.spark._collect_metrics(data=data, dimensions=self.dimensions)
            metrics_name = [x[0] for x in self.spark.aggregator.metrics.keys()]

        for metric in METRICS:
            self.assertIn(metric, metrics_name)

    def test_duration_time(self):
        endTime = datetime.datetime.now()
        startTime = (endTime - datetime.timedelta(seconds=30)).isoformat()
        endTime = endTime.isoformat()

        duration = self.spark._duration_time(startTime, endTime)

        self.assertEqual(duration, 30)

    def test_copy_dimensions(self):
        orig_dimension = {'a': 1}
        new_dimension = self.spark._update_dimension(orig_dimension, {'b': 2})
        self.assertEqual(new_dimension, {'a': 1, 'b': 2})
