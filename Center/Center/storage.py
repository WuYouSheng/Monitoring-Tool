#!/usr/bin/env python3
"""
Storage layer for QoS monitoring system.
Handles InfluxDB and MongoDB operations with clean separation.
"""

import json
from typing import Dict, Any, Optional, List
from pathlib import Path

from common import FrameMetric, QoSStats, current_timestamp_ms

try:
    from influxdb_client import InfluxDBClient, Point
    from influxdb_client.client.write_api import SYNCHRONOUS
except ImportError:
    InfluxDBClient = None
    Point = None

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None


class InfluxWriter:
    """Handles all InfluxDB operations."""

    def __init__(self, config: Dict[str, Any]):
        self.enabled = False
        self.write_api = None
        self.bucket = None
        self.org = None

        if config and InfluxDBClient and Point:
            self._connect(config)

    def _connect(self, config: Dict[str, Any]) -> None:
        try:
            url = f"http://{config['host']}:{config['port']}"
            client = InfluxDBClient(
                url=url,
                token=config['token'],
                org=config['org']
            )
            self.write_api = client.write_api(write_options=SYNCHRONOUS)
            self.bucket = config['bucket']
            self.org = config['org']
            self.enabled = True
        except Exception:
            self.enabled = False

    def write_host_metrics(self, uuid_str: str, host: str, metrics: Dict[str, Any]) -> None:
        if not self.enabled:
            return

        try:
            # Get service type from session_id
            service_type = metrics.get("session_id", "unknown")

            # Per-type FPS/throughput
            for frame_type in ["2D", "3D"]:
                point = (
                    Point("qos")
                    .tag("uuid", uuid_str)
                    .tag("host", host)
                    .tag("service", service_type)
                    .tag("type", frame_type)
                    .field("fps", float(metrics[f"fps_{frame_type.lower()}"]))
                    .field("throughput", int(metrics[f"count_{frame_type.lower()}"]))
                )
                self.write_api.write(bucket=self.bucket, record=point)

            # Host-level metrics
            host_point = (
                Point("qos")
                .tag("uuid", uuid_str)
                .tag("host", host)
                .tag("service", service_type)
                .tag("type", "HOST")
                .field("availability", float(metrics["availability"]))
                .field("host_fps_ok", float(metrics["fps_ok"]))
                .field("host_throughput_ok", float(metrics["throughput_ok"]))
                .field("host_network_ok", float(metrics["network_ok"]))
                .field("host_center_local_ok", float(metrics["local_ok"]))
                .field("host_failed_checks", metrics["failed_checks"])
            )

            # Add latency fields if available
            for prefix in ["network_latency", "local_latency", "sender_local_latency"]:
                stats = metrics.get(f"{prefix}_stats")
                if stats and isinstance(stats, QoSStats):
                    if stats.min_val is not None:
                        host_point = host_point.field(f"{prefix}_min", stats.min_val)
                    if stats.avg_val is not None:
                        host_point = host_point.field(f"{prefix}_avg", stats.avg_val)
                    if stats.max_val is not None:
                        host_point = host_point.field(f"{prefix}_max", stats.max_val)

            self.write_api.write(bucket=self.bucket, record=host_point)
        except Exception:
            pass

    def write_global_metrics(self, metrics: Dict[str, Any]) -> None:
        if not self.enabled:
            return

        try:
            # Global FPS/throughput for each type
            for frame_type in ["2D", "3D", "ALL"]:
                point = (
                    Point("qos")
                    .tag("uuid", "ALL")
                    .tag("host", "ALL")
                    .tag("type", frame_type)
                    .field("fps", float(metrics[f"fps_{frame_type.lower()}"]))
                    .field("throughput", int(metrics[f"count_{frame_type.lower()}"]))
                )
                self.write_api.write(bucket=self.bucket, record=point)

            # Global availability
            availability_point = (
                Point("qos")
                .tag("uuid", "ALL")
                .tag("host", "ALL")
                .tag("type", "ALL")
                .field("availability", float(metrics["availability"]))
                .field("fps_ok", float(metrics["fps_ok"]))
                .field("throughput_ok", float(metrics["throughput_ok"]))
                .field("network_ok_all_hosts", float(metrics["network_ok"]))
                .field("sender_local_ok_all_hosts", float(metrics["sender_local_ok"]))
                .field("center_local_ok", float(metrics["center_local_ok"]))
                .field("failed_checks", metrics["failed_checks"])
            )
            self.write_api.write(bucket=self.bucket, record=availability_point)
        except Exception:
            pass

    def write_cross_edge_timing(self, device_a: str, device_b: str,
                               frame_type: str, metrics: Dict[str, int]) -> None:
        if not self.enabled:
            return

        try:
            point = (
                Point("cross_edge_timing")
                .tag("device_a", device_a)
                .tag("device_b", device_b)
                .tag("frame_type", frame_type)
                .field("local_latency_a_ms", metrics["local_latency_a"])
                .field("local_latency_b_ms", metrics["local_latency_b"])
                .field("cross_network_latency_ms", metrics["cross_network_latency"])
            )
            self.write_api.write(bucket=self.bucket, record=point)
        except Exception:
            pass


class MongoWriter:
    """Handles all MongoDB operations."""

    def __init__(self, config: Dict[str, Any]):
        self.enabled = False
        self.client = None
        self.db = None

        if config and MongoClient:
            self._connect(config)

    def _connect(self, config: Dict[str, Any]) -> None:
        try:
            host = config.get("host", "localhost")
            port = config.get("port", 27017)
            database = config.get("database", "agent_tool")
            username = config.get("username")
            password = config.get("password")

            if username and password:
                uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"
            else:
                uri = f"mongodb://{host}:{port}/{database}"

            self.client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            self.client.admin.command("ping")
            self.db = self.client[database]
            self.enabled = True
        except Exception:
            self.enabled = False

    def save_json_file(self, file_data: Dict[str, Any]) -> None:
        if not self.enabled or self.db is None:
            return

        try:
            self.db["json_files"].insert_one(file_data)
        except Exception:
            pass

    def get_recent_docs(self, time_window_ms: int = 30000) -> List[Dict[str, Any]]:
        if not self.enabled or self.db is None:
            return []

        try:
            recent_time = current_timestamp_ms() - time_window_ms
            return list(self.db["json_files"].find({
                "received_time": {"$gte": recent_time}
            }).sort("received_time", -1))
        except Exception:
            return []


class FileStorage:
    """Handles file system operations."""

    def __init__(self, data_root: Path):
        self.data_root = data_root

    def ensure_client_dirs(self, uuid_str: str, host: str) -> Path:
        client_dir = self.data_root / uuid_str / host
        for subdir in ["2D", "3D"]:
            (client_dir / subdir).mkdir(parents=True, exist_ok=True)
        return client_dir

    def save_file(self, file_path: Path, content: str) -> bool:
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            return True
        except Exception:
            return False