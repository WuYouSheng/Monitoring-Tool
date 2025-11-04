#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from edge_instrumentation.auto_instrumentation.metricrecorder import MetricRecorder


def load_config():
    """Load config from center_config.json"""
    config_path = Path(__file__).parent / "center_config.json"
    with open(config_path) as f:
        return json.load(f)


def get_processing_latency():
    """Get center processing latency from InfluxDB"""
    try:
        config = load_config()
        influx_config = config.get("influxdb", {})

        from influxdb_client import InfluxDBClient

        url = f"http://{influx_config['host']}:{influx_config['port']}"
        client = InfluxDBClient(
            url=url,
            token=influx_config['token'],
            org=influx_config['org']
        )

        query_api = client.query_api()
        query = f'''
        from(bucket: "{influx_config['bucket']}")
        |> range(start: -5m)
        |> filter(fn: (r) => r._measurement == "qos" and r.type == "HOST" and r._field == "local_latency_avg")
        |> last()
        '''

        result = query_api.query(query)

        for table in result:
            for record in table.records:
                if record.get_value() is not None:
                    return float(record.get_value())

        return 0.0

    except Exception as e:
        print(f"Failed to query InfluxDB: {e}")
        return 0.0


def setup_otel_env(config):
    """Setup OpenTelemetry environment variables"""
    otel_config = config.get("otel", {})
    if not otel_config.get("enabled", False):
        return False

    os.environ["OTEL_SERVICE_NAME"] = otel_config.get("service_name", "agent-tool-center")
    os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = f"http://{otel_config.get('endpoint', '125.227.156.108:26001')}"
    return True


def run_with_instrumentation():
    """Run main with edge_opentelemetry_instrument"""
    config = load_config()

    if not setup_otel_env(config):
        print("OTel disabled in config")
        return

    print("Starting without automatic instrumentation...")
    main()


def main():
    """Main function that sends metrics"""
    metricRec = MetricRecorder()

    while True:
        processing_delay = get_processing_latency()

        metricRec.add_histogram(
            name="duration",
            unit="ms",
            value=processing_delay
        )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--direct":
        main()
    else:
        run_with_instrumentation()
