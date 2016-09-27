# (C) Copyright 2016 Guilherme Maluf Balzana <guimalufb at gmail.com>

import logging

from monasca_setup import agent_config
from monasca_setup.detection import Plugin
import monasca_setup.detection.utils as utils

_SPARK_JAVA_CLASS = 'org.apache.spark.deploy.history.HistoryServer'
_SPARK_HISTORY_PORT = 18080

log = logging.getLogger(__name__)


class Spark(Plugin):
    """Detect if Spark history server is running and creates the config file
    for getting metrics of it
    """

    def _detect(self):
        """Run detection, check if Spark history server is listen on history_port
        or process is running
        """

        history_port = _SPARK_HISTORY_PORT
        if self.args is not None:
            history_port = self.args.get('history_port', _SPARK_HISTORY_PORT)

        port_listening = utils.find_addr_listening_on_port(history_port) is not None
        process_exists = utils.find_process_cmdline(_SPARK_JAVA_CLASS) is not None
        has_dependencies = self.dependencies_installed()

        self.available = (port_listening or process_exists) and has_dependencies

        if not self.available:
            err_str = 'Spark history server plugin will not be configured.'
            if not process_exists:
                log.error('Spark history server not found. %s' % err_str)
            elif not port_listening:
                log.error('Spark history server exists but not listen on port %d. %s' %
                          (history_port, err_str))

    def build_config(self):
        """all args are optional and can be used as
         -a history_server_url="http://my-history-server:12345/api/v1" min_date="2016-10-13"
         """

        log.info('\tWatching Spark history server.')
        self.min_date = None
        self.history_server_url = 'http://localhost:18080/api/v1'

        if self.args is not None:
            self.min_date = self.args.get('min_date', self.min_date)
            self.history_server_url = self.args.get('history_server_url', self.history_server_url)

        config = agent_config.Plugins()
        config['spark'] = {
            'init_config': None,
            'instances': [{
                'history_server_url': self.history_server_url,
                'min_date': self.min_date
            }]
        }
        return config

    def dependencies_installed(self):
        return True
