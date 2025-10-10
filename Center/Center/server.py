#!/usr/bin/env python3
"""
Agent-Tool QoS Monitoring Center Server - Refactored
Clean architecture with proper separation of concerns.
"""

import asyncio
import json
import uuid
from pathlib import Path
from typing import Dict, Any

from aiohttp import web
from aiohttp import WSMsgType

import sys
sys.path.append(str(Path(__file__).parent.parent))

from common import determine_frame_type, current_timestamp_ms
from storage import InfluxWriter, MongoWriter, FileStorage
from metrics import MetricsCalculator, CrossEdgeAnalyzer


CONFIG_PATH = Path(__file__).with_name("center_config.json")
DATA_ROOT = Path(__file__).with_name("data")


def load_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


class QoSMonitor:
    """
    Main QoS monitoring coordinator.
    Clean separation: this class only handles HTTP endpoints and coordination.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.interval_seconds = int(config.get("monitor_interval", 5))
        self.edge_allowed_hosts = set((config.get("edge") or {}).values())

        # Session-based UUID management: (IP, session_id) -> UUID
        self.session_uuid_map: Dict[tuple, str] = {}

        # Components with single responsibilities
        self.influx_writer = InfluxWriter(config.get("influxdb"))
        self.mongo_writer = MongoWriter(config.get("mongodb"))
        self.file_storage = FileStorage(DATA_ROOT)
        self.metrics_calc = MetricsCalculator(config)
        self.cross_edge_analyzer = CrossEdgeAnalyzer()


    # HTTP endpoints removed; WebSocket endpoint remains below

    async def handle_ws(self, request) -> web.WebSocketResponse:
        """WebSocket endpoint for high-throughput streaming with minimal overhead."""
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)

        registered_uuid = None
        client_host = None
        try:
            peername = request.transport.get_extra_info("peername") if request and request.transport else None
            peer_ip = (peername[0] if isinstance(peername, tuple) else None) or getattr(request, "remote", None)
        except Exception:
            peer_ip = None

        if self.config.get("debug", False):
            print(f"[WS OPEN] peer_ip={peer_ip}")

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except Exception:
                        await ws.send_json({"type": "error", "error": "invalid_json"})
                        continue

                    msg_type = str(data.get("type", ""))
                    if self.config.get("debug", False):
                        print(f"[WS MSG] type={msg_type}")

                    # First message must be hello for registration
                    if registered_uuid is None:
                        if msg_type != "hello":
                            await ws.send_json({"type": "hello_ack", "accepted": False, "error": "not_authenticated"})
                            continue

                        host = str(data.get("host", "")).strip()
                        session_id = str(data.get("session_id", "default")).strip()

                        # Allow if allowlist empty or contains wildcard "*"
                        allow_any = (not self.edge_allowed_hosts) or ("*" in self.edge_allowed_hosts)
                        # Also accept if the actual peer IP is in allowlist
                        peer_allowed = bool(peer_ip and peer_ip in self.edge_allowed_hosts)
                        if not (allow_any or host in self.edge_allowed_hosts or peer_allowed):
                            if self.config.get("debug", False):
                                print(f"[WS HELLO] rejected host={host}, peer_ip={peer_ip}, allowlist={self.edge_allowed_hosts}")
                            await ws.send_json({"type": "hello_ack", "accepted": False})
                            continue

                        # Session-based UUID assignment
                        session_key = (peer_ip or host, session_id)
                        if session_key in self.session_uuid_map:
                            # Reuse existing UUID for this session
                            registered_uuid = self.session_uuid_map[session_key]
                        else:
                            # Create new UUID for this session
                            registered_uuid = str(uuid.uuid4())
                            self.session_uuid_map[session_key] = registered_uuid

                            # Only register client once per session
                            client_dir = self.file_storage.ensure_client_dirs(registered_uuid, host)
                            self.metrics_calc.register_client(registered_uuid, host, client_dir, session_id)

                        client_host = host
                        if self.config.get("debug", False):
                            print(f"[WS HELLO] accepted host={host}, session={session_id}, uuid={registered_uuid}")
                        await ws.send_json({"type": "hello_ack", "accepted": True, "uuid": registered_uuid})
                        continue

                    # After registration, accept single or batched files
                    if msg_type == "file":
                        await self._process_incoming_file(registered_uuid, client_host, data)
                        await ws.send_json({"type": "file_ack", "ok": True})
                    elif msg_type == "files":
                        items = data.get("items", [])
                        batch_id = data.get("batch_id")
                        ok_count = 0
                        for item in items:
                            try:
                                await self._process_incoming_file(registered_uuid, client_host, item)
                                ok_count += 1
                            except Exception:
                                pass
                        ack_payload = {"type": "files_ack", "ok": True, "count": ok_count}
                        if batch_id is not None:
                            ack_payload["batch_id"] = batch_id
                        await ws.send_json(ack_payload)
                    else:
                        await ws.send_json({"type": "error", "error": "unknown_type"})

                elif msg.type == WSMsgType.ERROR:
                    break
        finally:
            await ws.close()

        return ws

    async def _process_incoming_file(self, uuid_str: str, host: str, data: Dict[str, Any]) -> None:
        """Common routine to store content and update metrics (WS path)."""
        intercept_ms = current_timestamp_ms()

        if uuid_str not in self.metrics_calc.clients:
            raise RuntimeError("not_authenticated")

        filename = str(data.get("filename", ""))
        content_str = data.get("content", "")
        path_type = str(data.get("path_type", ""))

        state = self.metrics_calc.clients[uuid_str]
        frame_type = determine_frame_type(filename)
        dest_path = state.save_dir / frame_type.value / Path(filename).name

        content_to_save = content_str if isinstance(content_str, str) else json.dumps(content_str, ensure_ascii=False)
        if not self.file_storage.save_file(dest_path, content_to_save):
            raise RuntimeError("save_failed")

        from common import TimestampInfo
        ts_info = TimestampInfo.from_filename(filename)
        mongo_doc = {
            "uuid": uuid_str,
            "host": host,
            "filename": filename,
            "path_type": path_type,
            "received_time": intercept_ms,
            "content": content_to_save,
            "ts1": ts_info.ts1,
            "ts2": ts_info.ts2,
            "frame_type": frame_type.value
        }
        self.mongo_writer.save_json_file(mongo_doc)

        processing_end_ms = current_timestamp_ms()
        self.metrics_calc.add_frame_metric(
            uuid_str, filename, content_to_save, intercept_ms, processing_end_ms
        )

    async def interval_task(self):
        """Main monitoring loop. Simple and clean."""
        while True:
            await asyncio.sleep(self.interval_seconds)
            await self._process_interval()

    async def _process_interval(self):
        """Process one monitoring interval. Each component handles its own logic."""
        # Get metrics from calculator
        results = self.metrics_calc.flush_interval_metrics()

        # Write host metrics to InfluxDB
        for host_metrics in results["host_metrics"]:
            self.influx_writer.write_host_metrics(
                host_metrics["uuid"],
                host_metrics["host"],
                host_metrics
            )


            # Log failed availability checks
            if host_metrics["availability"] == 0.0 and host_metrics["failed_checks"]:
                self._log_host_failure(host_metrics)

        # Write global metrics
        self.influx_writer.write_global_metrics(results["global_metrics"])


        # Log global failures
        global_metrics = results["global_metrics"]
        if global_metrics["availability"] == 0.0 and global_metrics["failed_checks"]:
            self._log_global_failure(global_metrics)

        # Process cross-edge timing
        await self._process_cross_edge_timing()

    def _log_host_failure(self, metrics: Dict[str, Any]) -> None:
        """Log host-level availability failures."""
        print(f"[Host Availability FAIL] host={metrics['host']}, "
              f"uuid={metrics['uuid']}, failed_checks={metrics['failed_checks']}, "
              f"fps={metrics['fps_all']:.3f}, throughput={metrics['count_all']}")

    def _log_global_failure(self, metrics: Dict[str, Any]) -> None:
        """Log global availability failures."""
        print(f"[Availability FAIL] failed_checks={metrics['failed_checks']}, "
              f"total_fps={metrics['fps_all']:.3f}, total_throughput={metrics['count_all']}")

    async def _process_cross_edge_timing(self) -> None:
        """Process cross-edge timing analysis with debug output."""
        recent_docs = self.mongo_writer.get_recent_docs()
        cross_metrics = self.cross_edge_analyzer.analyze_cross_edge_timing(recent_docs)

        if self.config.get("debug", False):
            print(f"[DEBUG] Cross-edge analysis: {len(recent_docs)} recent docs, {len(cross_metrics)} metrics generated")

        for metric in cross_metrics:
            if self.config.get("debug", False):
                print(f"[DEBUG] Writing cross-edge metric: {metric}")

            self.influx_writer.write_cross_edge_timing(
                metric["device_a"],
                metric["device_b"],
                metric["frame_type"],
                metric
            )


async def main():
    config = load_config()
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    server_port = int(config.get("http_server_port", 444))
    qos = QoSMonitor(config)

    app = web.Application()
    app.router.add_get("/ws", qos.handle_ws)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=server_port)
    await site.start()

    print(f"HTTP server started on http://0.0.0.0:{server_port}")
    print("Endpoints:")
    print(f"  GET /ws - WebSocket streaming endpoint")

    task = asyncio.create_task(qos.interval_task())
    try:
        await asyncio.Future()  # run forever
    finally:
        print("\n[SHUTDOWN] Stopping QoS Monitor...")
        task.cancel()
        await runner.cleanup()
        print("[SHUTDOWN] Clean shutdown completed")


if __name__ == "__main__":
    asyncio.run(main())