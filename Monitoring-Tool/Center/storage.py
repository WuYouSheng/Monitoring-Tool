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
        except Exception as e:
            self.enabled = False
            print(f"[InfluxDB Connection Error] {e}")


    def write_batch_metrics(self, host_metrics_list: List[Dict[str, Any]], global_metrics: Dict[str, Any], timestamp_ns: int) -> None:
        """Write both host and global metrics in a single batch to ensure consistency."""
        if not self.enabled:
            return

        try:
            points = []

            # 1. Prepare Host Points
            for metrics in host_metrics_list:
                uuid_str = metrics["uuid"]
                host = metrics["host"]
                service_type = metrics.get("session_id", "unknown")

                # Per-type FPS/throughput
                for frame_type in ["2D", "3D"]:
                    p = (
                        Point("qos_v2")
                        .tag("uuid", uuid_str)
                        .tag("host", host)
                        .tag("service", service_type)
                        .tag("type", frame_type)
                        .field("fps", float(metrics[f"fps_{frame_type.lower()}"]))
                        .field("throughput", int(metrics[f"count_{frame_type.lower()}"]))
                        .time(timestamp_ns)
                    )
                    points.append(p)

                # Host-level metrics
                host_point = (
                    Point("qos_v2")
                    .tag("uuid", uuid_str)
                    .tag("host", host)
                    .tag("service", service_type)
                    .tag("type", "HOST")
                )
                
                if metrics["availability"] is not None:
                    host_point = host_point.field("availability", float(metrics["availability"]))

                host_point = (
                    host_point
                    .field("host_fps_ok", float(metrics["fps_ok"]))
                    .field("host_throughput_ok", float(metrics["throughput_ok"]))
                    .field("host_network_ok", float(metrics["network_ok"]))
                    .field("host_center_local_ok", float(metrics["local_ok"]))
                    .field("host_failed_checks", str(metrics["failed_checks"]))
                )
                
                # Add latency fields
                for prefix in ["network_latency", "local_latency", "sender_local_latency"]:
                    stats = metrics.get(f"{prefix}_stats")
                    if stats and isinstance(stats, QoSStats):
                        if stats.min_val is not None:
                            host_point = host_point.field(f"{prefix}_min", stats.min_val)
                        if stats.avg_val is not None:
                            host_point = host_point.field(f"{prefix}_avg", stats.avg_val)
                        if stats.max_val is not None:
                            host_point = host_point.field(f"{prefix}_max", stats.max_val)
                
                host_point = host_point.time(timestamp_ns)
                points.append(host_point)

            # 2. Prepare Global Points
            # Global FPS/throughput for each type
            for frame_type in ["2D", "3D", "ALL"]:
                p = (
                    Point("qos_v2")
                    .tag("uuid", "ALL")
                    .tag("host", "ALL")
                    .tag("type", frame_type)
                    .field("fps", float(global_metrics[f"fps_{frame_type.lower()}"]))
                    .field("throughput", int(global_metrics[f"count_{frame_type.lower()}"]))
                    .time(timestamp_ns)
                )
                points.append(p)

            # Global availability
            availability_point = (
                Point("qos_v2")
                .tag("uuid", "ALL")
                .tag("host", "ALL")
                .tag("type", "ALL")
            )
            
            if global_metrics["availability"] is not None:
                availability_point = availability_point.field("availability", float(global_metrics["availability"]))

            availability_point = (
                availability_point
                .field("fps_ok", float(global_metrics["fps_ok"]))
                .field("throughput_ok", float(global_metrics["throughput_ok"]))
                .field("network_ok_all_hosts", float(global_metrics["network_ok"]))
                .field("sender_local_ok_all_hosts", float(global_metrics["sender_local_ok"]))
                .field("center_local_ok", float(global_metrics["center_local_ok"]))
                .field("failed_checks", str(global_metrics["failed_checks"]))
                .time(timestamp_ns)
            )
            points.append(availability_point)

            # --- DEBUG: Print points before writing ---
            if True: # Set to False to disable debug print
                print(f"[BATCH WRITE DEBUG] Timestamp: {timestamp_ns} (Points count: {len(points)})")
                for p in points:
                    print(f"  - Point: {p.to_line_protocol()}")
            # --- END DEBUG ---

            # 3. Write Batch
            self.write_api.write(bucket=self.bucket, record=points)
            
        except Exception as e:
            print(f"[InfluxDB Batch Write Error] {e}")


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
        except Exception as e:
            print(f"[InfluxDB Cross Edge Write Error] {e}")


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
        except Exception as e:
            self.enabled = False
            print(f"[MongoDB Connection Error] {e}")

    def save_json_file(self, file_data: Dict[str, Any]) -> None:
        if not self.enabled or self.db is None:
            return

        try:
            self.db["json_files"].insert_one(file_data)
        except Exception as e:
            print(f"[MongoDB Save Error] {e}")

    def get_recent_docs(self, time_window_ms: int = 30000) -> List[Dict[str, Any]]:
        if not self.enabled or self.db is None:
            return []

        try:
            recent_time = current_timestamp_ms() - time_window_ms
            return list(self.db["json_files"].find({
                "received_time": {"$gte": recent_time}
            }).sort("received_time", -1))
        except Exception as e:
            print(f"[MongoDB Query Error] {e}")
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
