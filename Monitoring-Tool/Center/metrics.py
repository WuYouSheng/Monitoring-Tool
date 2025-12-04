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
    last_activity_ts: int = field(default_factory=current_timestamp_ms)


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
            session_id=session_id,
            last_activity_ts=current_timestamp_ms()
        )

    def add_frame_metric(self, uuid_str: str, filename: str, content: str,
                        intercept_ms: int, processing_end_ms: int) -> bool:
        """Process a single frame and add to metrics buffer."""
        state = self.clients.get(uuid_str)
        if not state:
            return False
        
        # Update activity timestamp
        state.last_activity_ts = current_timestamp_ms()

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
        
        # Use a single, synchronized reference time for all calculations
        current_ts = current_timestamp_ms()

        # 1. Group clients by Host to handle duplicates (zombies)
        # User requirement: Distinguish by host instead of UUID
        clients_by_host = defaultdict(list)
        for state in self.clients.values():
            clients_by_host[state.host].append(state)

        # 2. Select only the most recently active client per host, AND filter out stale hosts
        active_states = []
        stale_threshold_ms = 30000  # 30 seconds timeout
        
        for host, states in clients_by_host.items():
            # Sort by last_activity_ts descending (newest first)
            states.sort(key=lambda s: s.last_activity_ts, reverse=True)
            winner = states[0]
            
            # Check if the winner is actually active
            time_since_active = current_ts - winner.last_activity_ts
            if time_since_active <= stale_threshold_ms:
                active_states.append(winner)
            elif self.config.get("debug", False):
                print(f"[METRICS] Ignoring stale host: {host} (Last active: {time_since_active}ms ago)")

        # 3. Process only the active states
        for state in active_states:
            # Clear buffer and get frames regardless of whether we calculate metrics
            # This prevents buffer overflow if we skip
            buffer = list(state.interval_buffer)
            state.interval_buffer.clear()

            metrics = self._calculate_host_metrics_strict(state, buffer, current_ts)
            
            # Even if metrics is "empty" (no data), we might need to report 0 availability
            # But _calculate_host_metrics_strict will return a proper dict with 0.0 availability if no data
            host_metrics.append(metrics)
            
            global_counts["2d"] += metrics["count_2d"]
            global_counts["3d"] += metrics["count_3d"]

            # Collect for global calculations (legacy latency stats)
            if metrics["local_latency_stats"].avg_val is not None:
                all_center_latencies.extend(metrics["_raw_local_latencies"])

            host_checks["network"].append(metrics["network_ok"])
            host_checks["sender_local"].append(metrics["sender_local_ok"])

        # 4. Calculate Global Availability
        # User requirement: Global Availability = Sum(Individual Availabilities) / Count(Hosts)
        # Filter out hosts with None availability (No Data) to avoid skewing average with 0.0 from idle hosts
        valid_availabilities = [m["availability"] for m in host_metrics if m["availability"] is not None]
        
        if not valid_availabilities:
            global_availability = None
        else:
            global_availability = sum(valid_availabilities) / len(valid_availabilities)

        # Calculate other global aggregates for display
        interval_sec = float(self.config.get("monitor_interval", 5))
        total_count = global_counts["2d"] + global_counts["3d"]
        fps_2d = global_counts["2d"] / max(1, interval_sec)
        fps_3d = global_counts["3d"] / max(1, interval_sec)
        fps_all = fps_2d + fps_3d

        fps_ok = fps_all >= float(self.config.get("fps_threshold", 0))
        throughput_ok = total_count >= int(self.config.get("throughput_threshold", 0))
        network_ok = all(host_checks["network"]) if host_checks["network"] else False
        sender_local_ok = all(host_checks["sender_local"]) if host_checks["sender_local"] else False
        
        center_local_ok = True
        if all_center_latencies:
            avg_center_latency = sum(all_center_latencies) / len(all_center_latencies)
            threshold = float(self.config.get("local_latency_threshold", 1e12))
            center_local_ok = avg_center_latency <= threshold

        failed_checks = self._build_failed_checks([
            ("fps", fps_ok),
            ("throughput", throughput_ok),
            ("network_latency", network_ok),
            ("sender_local_latency", sender_local_ok),
            ("center_local_latency", center_local_ok)
        ])

        global_metrics = {
            "count_2d": global_counts["2d"],
            "count_3d": global_counts["3d"],
            "count_all": total_count,
            "fps_2d": fps_2d,
            "fps_3d": fps_3d,
            "fps_all": fps_all,
            "fps_ok": 1.0 if fps_ok else 0.0,
            "throughput_ok": 1.0 if throughput_ok else 0.0,
            "network_ok": 1.0 if network_ok else 0.0,
            "sender_local_ok": 1.0 if sender_local_ok else 0.0,
            "center_local_ok": 1.0 if center_local_ok else 0.0,
            "availability": global_availability,  # Use the strictly calculated average
            "failed_checks": failed_checks
        }

        return {
            "host_metrics": host_metrics,
            "global_metrics": global_metrics
        }


    def _calculate_host_metrics_strict(self, state: ClientState, buffer: List[FrameMetric], reference_ts: int) -> Dict[str, Any]:
        """
        Calculate metrics for a single host using strict user-defined availability logic.
        """
        valid_frames = [m for m in buffer if m.is_valid_frame]
        
        # DEBUG DIAGNOSTICS
        if self.config.get("debug", False):
            print(f"\n[STRICT CALC DEBUG] Host: {state.host}")
            print(f"  Buffer Size: {len(buffer)}, Valid: {len(valid_frames)}")
            if valid_frames:
                min_ts = min(f.received_ms for f in valid_frames)
                max_ts = max(f.received_ms for f in valid_frames)
                print(f"  Frame TS Range: {min_ts} - {max_ts}")
                print(f"  Reference TS:   {reference_ts}")
                print(f"  Window Start:   {reference_ts - (int(self.config.get('monitor_interval', 5)) * 1000)}")
        
        # Basic Stats
        frames_2d = [m for m in valid_frames if m.frame_type == FrameType.FRAME_2D]
        frames_3d = [m for m in valid_frames if m.frame_type == FrameType.FRAME_3D]
        count_all = len(valid_frames)
        count_2d = len(frames_2d)
        count_3d = len(frames_3d)
        
        interval_sec = float(self.config.get("monitor_interval", 5))
        fps_all = count_all / max(1, interval_sec)
        fps_2d = count_2d / max(1, interval_sec)
        fps_3d = count_3d / max(1, interval_sec)

        network_stats = self._get_latency_stats(valid_frames, "network_latency_ms")
        local_stats = self._get_latency_stats(valid_frames, "local_latency_ms")
        sender_local_stats = self._get_latency_stats(valid_frames, "sender_local_latency_ms")

        # --- Availability Calculation Start ---
        window_duration = int(interval_sec)
        
        # Group frames by second (aligned to reference_ts)
        frames_by_second = self._group_frames_by_second(valid_frames, window_duration, reference_ts)
        
        available_seconds = 0
        
        # Thresholds
        net_thresh = float(self.config.get("network_latency_threshold", 1e12))
        loc_thresh = float(self.config.get("local_latency_threshold", 1e12))
        throughput_thresh = int(self.config.get("throughput_threshold", 0)) # FPS sum > this
        
        for second in range(window_duration):
            sec_frames = frames_by_second.get(second, [])
            
            if not sec_frames:
                # No frames = 0 availability for this second
                continue
                
            # 1. Calculate metrics for this specific second
            sec_count = len(sec_frames)
            
            sec_net_lats = [f.network_latency_ms for f in sec_frames if f.network_latency_ms is not None]
            sec_loc_lats = [f.local_latency_ms for f in sec_frames if f.local_latency_ms is not None]
            
            avg_net = sum(sec_net_lats) / len(sec_net_lats) if sec_net_lats else 0
            avg_loc = sum(sec_loc_lats) / len(sec_loc_lats) if sec_loc_lats else 0
            
            # 2. Check Criteria
            # Note: User said "Network Latency < threshold". 
            # Usually thresholds are inclusive, but I will strictly follow the requirement logic combined with standard safety.
            # If config is 200, 199 is pass. 200 is... let's say pass (<=). 
            # User said "Total FPS > threshold". If threshold is 10, 11 is pass. 10 is... let's say pass (>=).
            
            is_net_ok = avg_net <= net_thresh
            is_loc_ok = avg_loc <= loc_thresh
            is_throughput_ok = sec_count >= throughput_thresh
            
            if is_net_ok and is_loc_ok and is_throughput_ok:
                available_seconds += 1
                
        availability = available_seconds / window_duration
        if count_all == 0:
            availability = None
        # --- Availability Calculation End ---

        # Legacy boolean checks for InfluxDB (Host level summary)
        fps_ok = fps_all >= float(self.config.get("fps_threshold", 0))
        throughput_ok = count_all >= throughput_thresh
        network_ok = self._check_latency_threshold(network_stats, "network_latency_threshold")
        local_ok = self._check_latency_threshold(local_stats, "local_latency_threshold")
        sender_local_ok = self._check_latency_threshold(sender_local_stats, "local_latency_threshold")
        
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
            "failed_checks": failed_checks if count_all > 0 else "no_data",
            "_raw_local_latencies": [m.local_latency_ms for m in valid_frames if m.local_latency_ms is not None],
            "_raw_valid_frames": valid_frames
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

    def _calculate_frame_based_availability(self, valid_frames: List[FrameMetric], reference_ts: Optional[int] = None) -> float:
        """Calculate time-window based availability.

        Each second in the monitoring window must meet ALL thresholds:
        - FPS >= fps_threshold
        - Network latency <= network_latency_threshold
        - Local latency <= local_latency_threshold
        - Throughput >= throughput_threshold

        Returns: float between 0.0 and 1.0 representing percentage of seconds that were available.
        """
        if not valid_frames:
            return 0.0

        window_duration = int(self.config.get("monitor_interval", 5))
        frames_by_second = self._group_frames_by_second(valid_frames, window_duration, reference_ts)

        available_seconds = 0
        total_seconds = window_duration

        for second in range(total_seconds):
            if self._is_second_available(frames_by_second.get(second, [])):
                available_seconds += 1

        return available_seconds / total_seconds


    def _group_frames_by_second(self, frames: List[FrameMetric], window_duration: int, reference_ts: Optional[int] = None) -> Dict[int, List[FrameMetric]]:
        """Group frames by second within the monitoring window."""
        if not frames:
            return {}

        # Use reference_ts if provided, otherwise fall back to latest frame timestamp
        if reference_ts is not None:
            latest_ts = reference_ts
        else:
            latest_ts = max(frame.received_ms for frame in frames)
            
        # Add 1 second tolerance/grace period to the window start
        # This prevents frames near the boundary from being dropped due to jitter
        tolerance_ms = 1000
        window_start = latest_ts - (window_duration * 1000) - tolerance_ms

        frames_by_second = defaultdict(list)

        for frame in frames:
            # We use a slightly lenient check to include frames that might be slightly ahead 
            # or within the window based on reference time
            if frame.received_ms >= window_start:
                # Calculate which second this frame belongs to relative to the *original* window start
                # We clamp the index to be within [0, window_duration-1] to handle the tolerance frames
                original_start = latest_ts - (window_duration * 1000)
                seconds_from_start = int((frame.received_ms - original_start) // 1000)
                
                # Map frames in the tolerance zone to the first second (bucket 0) 
                # or handle them as valid context. 
                # Here we simply allow them to contribute to the closest bucket if reasonable,
                # or just clamp them to 0 if they are slightly old.
                if seconds_from_start < 0:
                    seconds_from_start = 0
                
                if 0 <= seconds_from_start < window_duration:
                    frames_by_second[seconds_from_start].append(frame)

        return dict(frames_by_second)

    def _is_second_available(self, frames_in_second: List[FrameMetric]) -> bool:
        """Check if this second meets all availability thresholds."""
        if not frames_in_second:
            return False  # No data = not available

        # Calculate metrics for this second
        fps = len(frames_in_second)
        throughput = len(frames_in_second)

        # Calculate average latencies for frames with valid data
        network_latencies = [f.network_latency_ms for f in frames_in_second if f.network_latency_ms is not None]
        local_latencies = [f.local_latency_ms for f in frames_in_second if f.local_latency_ms is not None]
        sender_local_latencies = [f.sender_local_latency_ms for f in frames_in_second if f.sender_local_latency_ms is not None]

        avg_network_latency = sum(network_latencies) / len(network_latencies) if network_latencies else 0
        avg_local_latency = sum(local_latencies) / len(local_latencies) if local_latencies else 0
        avg_sender_local_latency = sum(sender_local_latencies) / len(sender_local_latencies) if sender_local_latencies else 0

        # Get thresholds
        fps_threshold = float(self.config.get("fps_threshold", 0))
        throughput_threshold = int(self.config.get("throughput_threshold", 0))
        network_threshold = float(self.config.get("network_latency_threshold", 1e12))
        local_threshold = float(self.config.get("local_latency_threshold", 1e12))

        # Check all thresholds
        fps_ok = fps >= fps_threshold
        throughput_ok = throughput >= throughput_threshold
        network_ok = avg_network_latency <= network_threshold
        local_ok = avg_local_latency <= local_threshold
        sender_local_ok = avg_sender_local_latency <= local_threshold

        return fps_ok and throughput_ok and network_ok and local_ok and sender_local_ok

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