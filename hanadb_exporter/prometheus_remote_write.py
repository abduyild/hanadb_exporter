from typing import Sequence
import snappy
from opentelemetry.exporter.prometheus_remote_write.gen.remote_pb2 import (
    WriteRequest,
)
from opentelemetry.exporter.prometheus_remote_write.gen.types_pb2 import (
    Label,
    Sample,
    TimeSeries,
)
import time


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

    
    def create_timeseries(self, name: str, value: float, extra_labels: dict):
        timeseries = TimeSeries()
        seen = set()

        def add_label(label_name: str, label_value: str):
            # Label name must contain only alphanumeric characters and underscores
            if label_name not in seen:
                label = Label()
                label.name = label_name
                label.value = label_value
                timeseries.labels.append(label)
                seen.add(label_name)
            else:
                logger.warning(
                    "Duplicate label with name %s and value %s",
                    label_name,
                    label_value,
                )

        # The __name__ label is required by PromQL as its value appears as the metric_name
        add_label("__name__", name)
        for label_key, label_value in extra_labels.items():
            add_label(label_key, label_value)

        sample = Sample()
        sample.timestamp = int(time.time() * 1000)
        sample.value = value
        timeseries.samples.append(sample)
        return timeseries