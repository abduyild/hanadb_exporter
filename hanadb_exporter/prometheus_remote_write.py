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