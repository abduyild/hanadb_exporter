"""
SAP HANA database prometheus data exporter app

:author: xarbulu
:organization: SUSE Linux GmbH
:contact: xarbulu@suse.de

:since: 2019-05-09
"""

import imp
from opentelemetry.exporter.prometheus_remote_write import (
    PrometheusRemoteWriteMetricsExporter
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry import metrics
from hanadb_exporter import secrets_manager
from hanadb_exporter import utils
from hanadb_exporter import db_manager
from hanadb_exporter import prometheus_exporter
from hanadb_exporter import __version__
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY
import sys
import os
import traceback
import logging
from logging.config import fileConfig
import time
import json
import argparse
from prometheus_remote_write import (
    PrometheusRemoteWrite,
    ExportResult
)

# getting the name of the directory
# where the this file is present.
current = os.path.dirname(os.path.realpath(__file__))

# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)

# adding the parent directory to
# the sys.path.
sys.path.append(parent)


LOGGER = logging.getLogger(__name__)
# in new systems /etc/ folder is not used in favor of /usr/etc
CONFIG_FILES_DIR = [
    '/etc/hanadb_exporter/',
    '/usr/etc/hanadb_exporter/'
]
METRICS_FILES = [
    '/etc/hanadb_exporter/metrics.json',
    '/usr/etc/hanadb_exporter/metrics.json'
]


def parse_config(config_file):
    """
    Parse config file
    """
    with open(config_file, 'r') as f_ptr:
        json_data = json.load(f_ptr)
    return json_data


def parse_arguments():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", help="Path to hanadb_exporter configuration file")
    parser.add_argument(
        "-m", "--metrics", help="Path to hanadb_exporter metrics file")
    parser.add_argument('-t', nargs=1, dest='api_token', action='store',
                        help='Sysdig API Token to use Prometheus Remote Write.')
    parser.add_argument('-u', nargs=1, dest='url', action='store',
                        help='Prometheus Remote Write complete url:path')
    parser.add_argument(
        "-d", "--daemon", action="store_true",
        help="Start the exporter as a systemd daemon. Only used when the the application "
             "is managed by systemd")
    parser.add_argument(
        "--identifier", help="Identifier of the configuration file from /etc/hanadb_exporter")
    parser.add_argument(
        "-v", "--verbosity",
        help="Python logging level. Options: DEBUG, INFO, WARN, ERROR (INFO by default)")
    parser.add_argument(
        "-V", "--version", action="store_true",
        help="Print the hanadb_exporter version")
    try:
        args = parser.parse_args()
    except BaseException as exception:
        exit(1)
    return args


def setup_logging(config):
    """
    Setup logging system
    """
    hana_config = config.get('hana')
    sufix = 'hanadb_exporter_{}_{}'.format(
        hana_config.get('host'), hana_config.get('port', 30015))
    log_file = config.get('logging').get(
        'log_file', '/var/log/{}'.format(sufix))

    fileConfig(config.get('logging').get('config_file'),
               defaults={'logfilename': log_file})

    # The next method is used to recatch and raise all
    # exceptions to redirect them to the logging system
    def handle_exception(*exc_info):  # pragma: no cover
        """
        Catch exceptions to log them
        """
        text = ''.join(traceback.format_exception(*exc_info))
        logging.getLogger('hanadb_exporter').error(text)

    sys.excepthook = handle_exception


def lookup_etc_folder(config_files_path):
    """
    Find predefined files in default locations (METRICS and CONFIG folder)
    This is used mainly because /etc location changed to /usr/etc in new systems
    return full filename path (e.g: /etc/hanadb_exporter/filename.json)
    """
    for conf_file in config_files_path:
        if os.path.isfile(conf_file):
            return conf_file
    raise ValueError(
        'configuration file does not exist in {}'.format(",".join(config_files_path)))

# Start up the server to expose the metrics.


def run():
    """
    Main execution
    """
    args = parse_arguments()
    if args.version:
        # pylint:disable=C0325
        print("hanadb_exporter %s" % (__version__))
        return
    if args.config is not None:
        config = parse_config(args.config)
    elif args.identifier is not None:
        file_name = args.identifier + '.json'
        # determine if file is /etc or /usr/etc
        config_file = lookup_etc_folder(
            [dirname + file_name for dirname in CONFIG_FILES_DIR])
        config = parse_config(config_file)

    else:
        raise ValueError('configuration file or identifier must be used')

    if config.get('logging', None):
        setup_logging(config)
    else:
        logging.basicConfig(level=args.verbosity or logging.INFO)

    if args.metrics:
        metrics = args.metrics
    else:
        metrics = lookup_etc_folder(METRICS_FILES)

    try:
        hana_config = config['hana']
        dbs = db_manager.DatabaseManager()
        user = hana_config.get('user', '')
        password = hana_config.get('password', '')
        userkey = hana_config.get('userkey', None)
        aws_secret_name = hana_config.get('aws_secret_name', '')

        if aws_secret_name:
            LOGGER.info(
                'AWS secret name is going to be used to read the database username and password')
            db_credentials = secrets_manager.get_db_credentials(
                aws_secret_name)
            user = db_credentials["username"]
            password = db_credentials["password"]

        dbs.start(
            hana_config['host'], hana_config.get('port', 30013),
            user=user,
            password=password,
            userkey=userkey,
            multi_tenant=config.get('multi_tenant', True),
            timeout=config.get('timeout', 30),
            ssl=hana_config.get('ssl', False),
            ssl_validate_cert=hana_config.get('ssl_validate_cert', False))
    except KeyError as err:
        raise KeyError(
            'Configuration file {} is malformed: {} not found'.format(args.config, err))

    if args.daemon:
        utils.systemd_ready()

    connectors = dbs.get_connectors()
    collector = prometheus_exporter.SapHanaCollectors(
        connectors=connectors, metrics_file=metrics)
    REGISTRY.register(collector)
    LOGGER.info('exporter successfully registered')

    LOGGER.info('starting to serve metrics')

    exporter = PrometheusRemoteWrite(args.url[0], args.api_token[0])
    headers = exporter.build_headers()

    metric_name = __name__

    your_value = 0
    extra_labels = {"my_labelname": "example", "other_label": "example_value2"}

    timeserie = exporter.create_timeseries(
        name=metric_name, value=your_value, extra_labels=extra_labels)
    metrics = exporter.build_message([timeserie])
    if exporter.send_message(metrics, headers) == ExportResult.SUCCESS:
        print("Metric: " + metric_name + " with labels: " + str(extra_labels))
    else:
        print("There was an error pushing the data")

    #start_http_server(config.get('exposition_port', 9668), config.get('listen_address', '0.0.0.0'))
    # while True:
    #    time.sleep(1)

    #start_http_server(config.get('exposition_port', 9668), config.get('listen_address', '0.0.0.0'))
    # while True:
    #    time.sleep(1)


if __name__ == "__main__":  # pragma: no cover
    run()
