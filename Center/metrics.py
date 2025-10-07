#!/usr/bin/env python3
"""
Metrics calculation and aggregation for QoS monitoring.
Clean separation of metric logic from I/O operations.
"""

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Deque, Any
from pathlib import Path

from common import FrameMetric, QoSStats, TimestampInfo, FrameType, current_timestamp_ms
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from latency_utils import create_robust_calculator, LatencyValidationResult


@dataclass
class ClientState:
    uuid_str: str
    host: str
    save_dir: Path
    session_id: str = "default"
    interval_buffer: Deque[FrameMetric] = field(default_factory=deque)
    min_ts_intercept_delta_ms: Optional[int] = None


class MetricsCalculator:
    """Handles all QoS metrics calculations with robust latency processing."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.clients: Dict[str, ClientState] = {}

        # Initialize robust latency calculator
        latency_config = {
            "clock_drift_tolerance_ms": config.get("clock_drift_tolerance_ms", 5000),
            "enable_outlier_filtering": config.get("enable_outlier_filtering", True),
            "outlier_method": config.get("outlier_method", "iqr")
        }
        self.latency_calculator = create_robust_calculator(latency_config)

    def register_client(self, uuid_str: str, host: str, save_dir: Path, session_id: str = "default") -> None:
        self.clients[uuid_str] = ClientState(
            uuid_str=uuid_str,
            host=host,
            save_dir=save_dir,
            session_id=session_id
        )

    def add_frame_metric(self, uuid_str: str, filename: str, content: str,
                        intercept_ms: int, processing_end_ms: int) -> bool:
        """Process a single frame and add to metrics buffer."""
        state = self.clients.get(uuid_str)
        if not state:
            return False

        # Parse timestamps
        ts_info = TimestampInfo.from_filename(filename)
        frame_type = self._determine_frame_type(filename)

        # Calculate latencies
        sender_local_latency_ms = self._calc_sender_latency(ts_info)
        network_latency_ms = self._calc_network_latency(state, ts_info, intercept_ms)
        # Calculate local latency using robust calculator
        local_result = self.latency_calculator.calculate_local_processing_latency(intercept_ms, processing_end_ms)
        local_latency_ms = int(local_result.value) if local_result.value is not None else 0

        # Log issues if in debug mode
        if local_result.issues and self.config.get("debug_latency_issues", False):
            print(f"[LATENCY] Local processing latency issues: {[i.value for i in local_result.issues]}")

        # Validate content
        is_valid = self._is_valid_json(content)

        # Add to buffer
        metric = FrameMetric(
            received_ms=intercept_ms,
            network_latency_ms=network_latency_ms,
            local_latency_ms=local_latency_ms,
            sender_local_latency_ms=sender_local_latency_ms,
            is_valid_frame=is_valid,
            frame_type=frame_type
        )

        state.interval_buffer.append(metric)
        return True

    def flush_interval_metrics(self) -> Dict[str, Any]:
        """Flush and calculate interval metrics for all clients."""
        host_metrics = []
        global_counts = {"2d": 0, "3d": 0}
        all_center_latencies = []
        host_checks = {"network": [], "sender_local": []}

        # Process each client
        for state in self.clients.values():
            metrics = self._calculate_host_metrics(state)
            if metrics:
                host_metrics.append(metrics)
                global_counts["2d"] += metrics["count_2d"]
                global_counts["3d"] += metrics["count_3d"]

                # Collect for global calculations
                if metrics["local_latency_stats"].avg_val is not None:
                    all_center_latencies.extend(metrics["_raw_local_latencies"])

                host_checks["network"].append(metrics["network_ok"])
                host_checks["sender_local"].append(metrics["sender_local_ok"])

        # Calculate global metrics
        global_metrics = self._calculate_global_metrics(
            global_counts, all_center_latencies, host_checks, host_metrics
        )

        return {
            "host_metrics": host_metrics,
            "global_metrics": global_metrics
        }

    def _calculate_host_metrics(self, state: ClientState) -> Optional[Dict[str, Any]]:
        """Calculate metrics for a single host."""
        buffer = list(state.interval_buffer)
        state.interval_buffer.clear()

        if not buffer:
            return None

        # Filter valid frames
        valid_frames = [m for m in buffer if m.is_valid_frame]
        frames_2d = [m for m in valid_frames if m.frame_type == FrameType.FRAME_2D]
        frames_3d = [m for m in valid_frames if m.frame_type == FrameType.FRAME_3D]

        interval_sec = float(self.config.get("monitor_interval", 5))
        count_all = len(valid_frames)
        count_2d = len(frames_2d)
        count_3d = len(frames_3d)

        fps_all = count_all / max(1, interval_sec)
        fps_2d = count_2d / max(1, interval_sec)
        fps_3d = count_3d / max(1, interval_sec)

        # Calculate latency stats
        network_stats = self._get_latency_stats(valid_frames, "network_latency_ms")
        local_stats = self._get_latency_stats(valid_frames, "local_latency_ms")
        sender_local_stats = self._get_latency_stats(valid_frames, "sender_local_latency_ms")

        # Check thresholds (binary for backward compatibility)
        fps_ok = fps_all >= float(self.config.get("fps_threshold", 0))
        throughput_ok = count_all >= int(self.config.get("throughput_threshold", 0))
        network_ok = self._check_latency_threshold(network_stats, "network_latency_threshold")
        local_ok = self._check_latency_threshold(local_stats, "local_latency_threshold")
        sender_local_ok = self._check_latency_threshold(sender_local_stats, "local_latency_threshold")

        # Calculate frame-based availability (time-proportional)
        availability = self._calculate_frame_based_availability(valid_frames)

        # Build failed checks string
        failed_checks = self._build_failed_checks([
            ("fps", fps_ok),
            ("throughput", throughput_ok),
            ("network_latency", network_ok),
            ("center_local_latency", local_ok)
        ])

        return {
            "uuid": state.uuid_str,
            "host": state.host,
            "session_id": state.session_id,
            "count_all": count_all,
            "count_2d": count_2d,
            "count_3d": count_3d,
            "fps_all": fps_all,
            "fps_2d": fps_2d,
            "fps_3d": fps_3d,
            "network_latency_stats": network_stats,
            "local_latency_stats": local_stats,
            "sender_local_latency_stats": sender_local_stats,
            "fps_ok": 1.0 if fps_ok else 0.0,
            "throughput_ok": 1.0 if throughput_ok else 0.0,
            "network_ok": 1.0 if network_ok else 0.0,
            "local_ok": 1.0 if local_ok else 0.0,
            "sender_local_ok": sender_local_ok,
            "availability": availability,
            "failed_checks": failed_checks,
            "_raw_local_latencies": [m.local_latency_ms for m in valid_frames if m.local_latency_ms is not None]
        }

    def _calculate_global_metrics(self, counts: Dict[str, int],
                                all_center_latencies: List[int],
                                host_checks: Dict[str, List[bool]],
                                host_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate global availability metrics."""
        interval_sec = float(self.config.get("monitor_interval", 5))
        total_count = counts["2d"] + counts["3d"]

        fps_2d = counts["2d"] / max(1, interval_sec)
        fps_3d = counts["3d"] / max(1, interval_sec)
        fps_all = fps_2d + fps_3d

        # Global checks
        fps_ok = fps_all >= float(self.config.get("fps_threshold", 0))
        throughput_ok = total_count >= int(self.config.get("throughput_threshold", 0))
        network_ok = all(host_checks["network"]) if host_checks["network"] else False
        sender_local_ok = all(host_checks["sender_local"]) if host_checks["sender_local"] else False

        # Center local latency check
        center_local_ok = True
        if all_center_latencies:
            avg_center_latency = sum(all_center_latencies) / len(all_center_latencies)
            threshold = float(self.config.get("local_latency_threshold", 1e12))
            center_local_ok = avg_center_latency <= threshold

        # Calculate global availability as average of host availabilities
        availability = self._calculate_global_frame_based_availability(host_metrics)

        failed_checks = self._build_failed_checks([
            ("fps", fps_ok),
            ("throughput", throughput_ok),
            ("network_latency", network_ok),
            ("sender_local_latency", sender_local_ok),
            ("center_local_latency", center_local_ok)
        ])

        return {
            "count_2d": counts["2d"],
            "count_3d": counts["3d"],
            "count_all": total_count,
            "fps_2d": fps_2d,
            "fps_3d": fps_3d,
            "fps_all": fps_all,
            "fps_ok": 1.0 if fps_ok else 0.0,
            "throughput_ok": 1.0 if throughput_ok else 0.0,
            "network_ok": 1.0 if network_ok else 0.0,
            "sender_local_ok": 1.0 if sender_local_ok else 0.0,
            "center_local_ok": 1.0 if center_local_ok else 0.0,
            "availability": availability,
            "failed_checks": failed_checks
        }

    def _determine_frame_type(self, filename: str) -> FrameType:
        if filename.startswith("3D_"):
            return FrameType.FRAME_3D
        elif filename.startswith("2D_"):
            return FrameType.FRAME_2D
        else:
            return FrameType.MISC

    def _calc_sender_latency(self, ts_info: TimestampInfo) -> Optional[int]:
        """Calculate sender latency using robust calculator."""
        result = self.latency_calculator.calculate_sender_local_latency(ts_info.ts1, ts_info.ts2)

        # Log issues if in debug mode
        if result.issues and self.config.get("debug_latency_issues", False):
            print(f"[LATENCY] Sender latency issues: {[i.value for i in result.issues]}")

        return int(result.value) if result.value is not None else None

    def _calc_network_latency(self, state: ClientState,
                            ts_info: TimestampInfo, intercept_ms: int) -> Optional[int]:
        """Calculate network latency using robust calculator with clock drift correction."""
        ts_for_net = ts_info.ts2 if ts_info.ts2 is not None else ts_info.ts1
        device_id = f"{state.uuid_str}_{state.host}"

        result = self.latency_calculator.calculate_network_latency(
            device_id, ts_for_net, intercept_ms
        )

        # Log issues if in debug mode
        if result.issues and self.config.get("debug_latency_issues", False):
            print(f"[LATENCY] Network latency issues for {device_id}: {[i.value for i in result.issues]}")

        return int(result.value) if result.value is not None else None

    def _is_valid_json(self, content: str) -> bool:
        try:
            import json
            if isinstance(content, str) and content:
                parsed = json.loads(content)
                return bool(parsed)
        except (json.JSONDecodeError, TypeError):
            pass
        return False

    def _get_latency_stats(self, frames: List[FrameMetric], attr: str) -> QoSStats:
        values = []
        for frame in frames:
            val = getattr(frame, attr, None)
            if val is not None:
                values.append(val)
        return QoSStats.from_values(values)

    def _check_latency_threshold(self, stats: QoSStats, threshold_key: str) -> bool:
        if stats.avg_val is None:
            return True
        threshold = float(self.config.get(threshold_key, 1e12))
        return stats.avg_val <= threshold

    def _calculate_frame_based_availability(self, valid_frames: List[FrameMetric]) -> float:
        """Calculate availability based on individual frame quality rather than aggregate.

        Returns: float between 0.0 and 1.0 representing the percentage of time the system was available.
        Each frame represents a moment in time - if the frame meets quality criteria, that moment is "available".
        If no frames are available, return 0.0 (no availability).
        """
        if not valid_frames:
            return 0.0

        total_frames = len(valid_frames)
        available_frames = 0

        # Thresholds
        network_threshold = float(self.config.get("network_latency_threshold", 1e12))
        local_threshold = float(self.config.get("local_latency_threshold", 1e12))

        for frame in valid_frames:
            # Check if this frame meets all quality criteria
            network_ok = (frame.network_latency_ms is None or
                         frame.network_latency_ms <= network_threshold)
            local_ok = (frame.local_latency_ms is None or
                       frame.local_latency_ms <= local_threshold)
            sender_local_ok = (frame.sender_local_latency_ms is None or
                              frame.sender_local_latency_ms <= local_threshold)

            # Frame is "available" if all latency criteria are met
            if network_ok and local_ok and sender_local_ok:
                available_frames += 1

        # Return the proportion of frames that were available
        availability_ratio = available_frames / total_frames
        return availability_ratio

    def _calculate_global_frame_based_availability(self, host_metrics: List[Dict[str, Any]]) -> float:
        """Calculate global availability as average of all host availabilities.

        Returns: float between 0.0 and 1.0 representing overall system availability.
        If no host data is available, return 0.0 (system unavailable).
        """
        if not host_metrics:
            return 0.0

        # Extract availability values from host metrics
        host_availabilities = []
        for host_metric in host_metrics:
            if "availability" in host_metric:
                host_availabilities.append(host_metric["availability"])

        if not host_availabilities:
            return 0.0

        # Return the average availability across all hosts
        return sum(host_availabilities) / len(host_availabilities)

    def _build_failed_checks(self, checks: List[tuple]) -> str:
        failed = [name for name, passed in checks if not passed]
        return ",".join(failed)


class CrossEdgeAnalyzer:
    """Handles cross-device timing analysis."""

    def __init__(self):
        pass

    def analyze_cross_edge_timing(self, recent_docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze timing across different edge devices."""
        if len(recent_docs) < 2:
            return []

        # Group by content
        content_groups = defaultdict(list)
        for doc in recent_docs:
            content_key = str(doc.get("content", ""))
            content_groups[content_key].append(doc)

        results = []

        # Process each content group
        for content_key, docs in content_groups.items():
            if len(docs) < 2:
                continue

            # Group by device
            device_groups = defaultdict(list)
            for doc in docs:
                device_key = f"{doc['host']}_{doc['uuid']}"
                device_groups[device_key].append(doc)

            if len(device_groups) < 2:
                continue

            # Calculate cross-device metrics
            devices = sorted(device_groups.keys())
            for i in range(len(devices)):
                for j in range(i + 1, len(devices)):
                    device_a, device_b = devices[i], devices[j]
                    doc_a = max(device_groups[device_a], key=lambda x: x['received_time'])
                    doc_b = max(device_groups[device_b], key=lambda x: x['received_time'])

                    metric = self._calculate_cross_device_metric(doc_a, doc_b, device_a, device_b)
                    if metric:
                        results.append(metric)

        return results

    def _calculate_cross_device_metric(self, doc_a: Dict[str, Any], doc_b: Dict[str, Any],
                                     device_a: str, device_b: str) -> Optional[Dict[str, Any]]:
        ts1_a, ts2_a = doc_a.get('ts1'), doc_a.get('ts2')
        ts1_b, ts2_b = doc_b.get('ts1'), doc_b.get('ts2')

        if not all([ts1_a, ts2_a, ts1_b, ts2_b]):
            return None

        return {
            "device_a": device_a,
            "device_b": device_b,
            "frame_type": doc_a.get('frame_type', 'unknown'),
            "local_latency_a": abs(ts1_a - ts2_a),
            "local_latency_b": abs(ts1_b - ts2_b),
            "cross_network_latency": abs(ts1_a - ts1_b)
        }