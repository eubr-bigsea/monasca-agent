# (C) Copyright 2016 Guilherme Maluf Balzana <guimalufb at gmail.com>

import datetime
import dateutil.parser
import requests
import time

from monasca_agent.collector.checks import AgentCheck

METRICS = {
    'application': {},
    'job': {
        'numActiveStages': 'num_active_stages',
        'numActiveTasks': 'num_active_tasks',
        'numCompletedStages': 'num_completed_stages',
        'numCompletedTasks': 'num_completed_tasks',
        'numFailedStages': 'num_failed_stages',
        'numFailedTasks': 'num_failed_tasks',
        'numSkippedStages': 'num_skipped_stages',
        'numSkippedTasks': 'num_skipped_tasks',
        'numTasks': 'num_tasks',
    },
    'stage': {
        'diskBytesSpilled': 'disk_bytes_spilled',
        'executorRunTime': 'executor_run_time',
        'inputBytes': 'input_bytes',
        'inputRecords': 'input_records',
        'memoryBytesSpilled': 'memory_bytes_spilled',
        'numActiveTasks': 'num_active_tasks',
        'numCompleteTasks': 'num_complete_tasks',
        'numFailedTasks': 'num_failed_tasks',
        'outputBytes': 'output_bytes',
        'outputRecords': 'output_records',
        'shuffleReadBytes': 'shuffle_read_bytes',
        'shuffleReadRecords': 'shuffle_read_records',
        'shuffleWriteBytes': 'shuffle_write_bytes',
        'shuffleWriteRecords': 'shuffle_write_records',
    },
    'executor': {
        'activeTasks': 'active_tasks',
        'completedTasks': 'completed_tasks',
        'diskUsed': 'disk_used',
        'failedTasks': 'failed_tasks',
        'maxMemory': 'max_memory',
        'memoryUsed': 'memory_used',
        'rddBlocks': 'rdd_blocks',
        'totalDuration': 'total_duration',
        'totalInputBytes': 'total_input_bytes',
        'totalShuffleRead': 'total_shuffle_read',
        'totalShuffleWrite': 'total_shuffle_write',
        'totalTasks': 'total_tasks',
    },
    'storage': {
        'diskUsed': 'disk_used',
        'memoryUsed': 'memory_used',
        'numCachedPartitions': 'num_cached_partitions',
        'numPartitions': 'num_partitions',
    },
}


class Spark(AgentCheck):
    """Extract metrics from Spark monitor REST API"""

    def check(self, instance):
        if self.instance_count() > 0:
            config_history_server_url = instance.get('history_server_url', None)
            if config_history_server_url is None:
                raise Exception('A Spark history_server_url must be specified')

            config_min_date = instance.get('min_date', None)
            if config_min_date is None:
                # Plugin config check_freq should have precedence over AgentConfig
                check_freq = self.init_config.get('check_freq',
                                                  self.agent_config.get('check_freq', 0))
                # Filter API time range with last plugin check
                last_check = (datetime.datetime.now() -
                              datetime.timedelta(seconds=check_freq)).isoformat()
            else:
                last_check = config_min_date

            self.log.debug("Fetching data since %s" % last_check)
        else:
            raise Exception('At least one instance should be defined')

        dimensions = self._set_dimensions({'component': 'spark_history',
                                          'service': 'spark'},
                                          instance)

        data = self._fetch_data(url=config_history_server_url, minDate=last_check)
        self._collect_metrics(data, dimensions)

    def _collect_metrics(self, data, dimensions, value_meta={}):
        for app in data:
            app_dimensions = self._update_dimension(dimensions, {'app_id': app['id'],
                                                                 'app_name': app['name']})

            duration = self._duration_time(app['attempts'][0]['startTime'],
                                           app['attempts'][0]['endTime'])
            start_time = self._unix_timestamp(app['attempts'][0]['startTime'])

            self.gauge('spark.application.duration', duration,
                       timestamp=start_time,
                       dimensions=app_dimensions,
                       value_meta=value_meta)

            for job in app['jobs']:
                job_dimensions = self._update_dimension(app_dimensions,
                                                        {'job_id': str(job['jobId']),
                                                         'job_name': job['name']})

                duration = self._duration_time(job['submissionTime'], job['completionTime'])
                job_submission_time = self._unix_timestamp(job['submissionTime'])

                self.gauge('spark.application.job.duration', duration,
                           timestamp=job_submission_time,
                           dimensions=job_dimensions,
                           value_meta=value_meta)

                self._proccess_metrics(job, METRICS['job'], 'job',
                                       job_dimensions, job_submission_time,
                                       value_meta)

                for stage in job['stageIds']:
                    self._proccess_metrics(app['stages'][stage], METRICS['stage'], 'job.stage',
                                           job_dimensions, job_submission_time,
                                           value_meta)

            for executor in app['executors']:
                executor_dimensions = self._update_dimension(app_dimensions,
                                                             {'executor_host_port':
                                                              executor['hostPort']})
                self._proccess_metrics(executor, METRICS['executor'], 'executor',
                                       executor_dimensions, start_time,
                                       value_meta)

            for storage in app['storages']:
                storage_dimensions = self._update_dimension(app_dimensions,
                                                            {'storage_name': storage['name']})
                self._proccess_metrics(storage, METRICS['storage'], 'storage',
                                       storage_dimensions, start_time,
                                       value_meta)

    def _proccess_metrics(self, data, metrics, name, dimensions, timestamp, value_meta):
        for spark_metric, metric_name in metrics.iteritems():
            self.gauge('spark.application.%s.%s' % (name, metric_name),
                       value=data[spark_metric],
                       dimensions=dimensions,
                       timestamp=timestamp,
                       value_meta=value_meta)

    def _update_dimension(self, dimension, update):
        new_dimension = dimension.copy()
        new_dimension.update(update)
        return new_dimension

    def _fetch_data(self, url, minDate=None):
        applications = self._get_applications(url=url, minDate=minDate)
        for app in applications:
            app['jobs'] = self._get_jobs(url=url, application_id=app['id'])
            app['stages'] = self._get_stages(url=url, application_id=app['id'])
            app['executors'] = self._get_executors(url=url, application_id=app['id'])
            app['storages'] = self._get_storages(url=url, application_id=app['id'])

        return applications

    def _get_api_request(self, url, path='', params={}):
        try:
            response = requests.get(url=url + path, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.log.error(e)
            self.log.error('Failed to GET %s with %s' % (url+path, params))
            return {}

    def _get_applications(self, url, status=None, minDate=None, maxDate=None):
        params = {
            'status': status,
            'minDate': minDate,
            'maxDate': maxDate
        }
        return self._get_api_request(url=url,
                                     path='/applications',
                                     params=params)

    def _get_jobs(self, url, application_id, status=None):
        params = {'status': status}
        return self._get_api_request(url=url,
                                     path='/applications/' + application_id + '/jobs',
                                     params=params)

    def _get_stages(self, url, application_id):
        return self._get_api_request(url=url,
                                     path='/applications/' + application_id + '/stages')

    def _get_stage(self, url, application_id, stage_id, status=None):
        params = {'status': status}
        return self._get_api_request(url=url,
                                     path='/applications/' + application_id +
                                          '/stages/' + str(stage_id),
                                     params=params)

    def _get_executors(self, url, application_id):
        return self._get_api_request(url=url,
                                     path='/applications/' + application_id +
                                          '/executors')

    def _get_storages(self, url, application_id):
        return self._get_api_request(url=url,
                                     path='/applications/' + application_id +
                                          '/storage/rdd')

    def _get_storage(self, url, application_id, rdd_id):
        return self._get_api_request(url=url,
                                     path='/applications/' + application_id +
                                          '/storage/rdd/' + str(rdd_id))

    def _unix_timestamp(self, date):
        return time.mktime(dateutil.parser.parse(date).timetuple())

    def _duration_time(self, startTime, endTime):
        eT = dateutil.parser.parse(endTime)
        sT = dateutil.parser.parse(startTime)
        return abs((eT - sT).total_seconds())
