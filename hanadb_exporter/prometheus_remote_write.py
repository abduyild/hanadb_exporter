from typing import Sequence
import snappy
from opentelemetry.exporter.prometheus_remote_write.gen.remote_pb2 import WriteRequest
from opentelemetry.exporter.prometheus_remote_write.gen.types_pb2 import (
    TimeSeries,
)


class PrometheusRemoteWrite():
    def __init__(self, url, bearer):
        self.url = url
        self.bearer = bearer
        self.timeout = 10

    def build_headers(self):
        headers = {
            "Content-Encoding": "snappy",
            "Content-Type": "application/x-protobuf",
            "X-Prometheus-Remote-Write-Version": "0.1.0",
            "Authorization": "Bearer " + self.bearer,
        }
        return headers

    def build_message(self, timeseries: Sequence[TimeSeries]):
        write_request = WriteRequest()
        write_request.timeseries.extend(timeseries)
        serialized_message = write_request.SerializeToString()
        return snappy.compress(serialized_message)