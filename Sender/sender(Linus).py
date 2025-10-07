#!/usr/bin/env python3

import asyncio
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import websockets
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import sys
sys.path.append(str(Path(__file__).parent.parent))

from common import current_timestamp_ms

CONFIG_PATH = Path(__file__).with_name("sender_config.json")


def load_config() -> Dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

class FilenameProcessor:

    @staticmethod
    def has_two_timestamps(stem: str) -> bool:
        return re.match(r"^[23]D_\d{10,20}_\d{10,20}$", stem) is not None

    @staticmethod
    def add_send_timestamp(path: Path) -> Path:
        if not path.exists():
            return path

        parent = path.parent
        stem = path.stem
        suffix = path.suffix

        if FilenameProcessor.has_two_timestamps(stem):
            return path

        # Pattern matching for single timestamp
        single_ts_match = re.match(r"^([23]D)_(\d{10,20})$", stem)
        if single_ts_match:
            prefix = single_ts_match.group(1)
            first_ts = single_ts_match.group(2)
            send_ms = current_timestamp_ms()
            new_name = f"{prefix}_{first_ts}_{send_ms}{suffix}"
        else:
            send_ms = current_timestamp_ms()
            new_name = f"{stem}_{send_ms}{suffix}"

        new_path = parent / new_name
        try:
            path.rename(new_path)
            return new_path
        except Exception:
            return path

class FileManager:

    @staticmethod
    def find_sent_files(dir_path: Path) -> List[Path]:
        if not dir_path.exists():
            return []

        return [
            p for p in dir_path.glob("*.json")
            if "_SENT" in p.name
        ]

    @staticmethod
    def find_unsent_files(dir_path: Path) -> List[Path]:
        if not dir_path.exists():
            return []

        return [
            p for p in dir_path.glob("*.json")
            if not p.name.endswith("_SENT.json") and "_SENT" not in p.stem
        ]

    @staticmethod
    def find_t2_files(dir_path: Path) -> List[Path]:
        if not dir_path.exists():
            return []

        return [
            p for p in dir_path.glob("*.json")
            if FilenameProcessor.has_two_timestamps(p.stem) and "_SENT" not in p.name
        ]

    @staticmethod
    def mark_as_sent(path: Path) -> None:
        if not path.exists():
            return

        parent = path.parent
        stem = path.stem
        suffix = path.suffix

        if not stem.endswith("_SENT"):
            new_name = f"{stem}_SENT{suffix}"
            try:
                path.rename(parent / new_name)
            except Exception:
                pass

    @staticmethod
    def delete_file(path: Path, retries: int = 3) -> bool:
        import time

        for attempt in range(retries):
            try:
                if not path.exists():
                    return True  # Already deleted
                path.unlink(missing_ok=True)
                return True
            except PermissionError as e:
                if attempt == retries - 1:  # Only report on last attempt
                    print(f"Delete failed: {path.name} - Permission denied after {retries} attempts: {e}")
                time.sleep(0.05)  # Fixed delay
            except Exception as e:
                if attempt == retries - 1:
                    print(f"Delete failed: {path.name} - {type(e).__name__}: {e}")
                time.sleep(0.05)
        return False

class FileEventHandler(FileSystemEventHandler):

    def __init__(self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue, path_type: str, sender: 'FileSender'):
        super().__init__()
        self.loop = loop
        self.queue = queue
        self.path_type = path_type
        self.sender = sender

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.json'):
            self._queue_file(event.src_path)

    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.json'):
            self._queue_file(event.src_path)

    def on_moved(self, event):
        # Also handle file moves (renames) for completeness
        if not event.is_directory and event.dest_path.endswith('.json'):
            self._queue_file(event.dest_path)

    def _queue_file(self, file_path: str) -> None:
        try:
            # Don't skip files - let queue handle backpressure naturally
            self.loop.call_soon_threadsafe(self._immediate_queue_sync, file_path)
        except Exception:
            pass

    def _immediate_queue_sync(self, file_path: str) -> None:
        try:
            self.loop.create_task(self._immediate_queue_async(file_path))
        except Exception:
            pass

    async def _immediate_queue_async(self, file_path: str) -> None:
        file_obj = Path(file_path)

        # Use absolute path as key - simpler and more reliable
        file_key = str(file_obj.absolute())
        if file_key in self.sender.processed_files:
            return

        try:
            if file_obj.exists() and file_obj.stat().st_size > 0:
                # Non-blocking put with backpressure handling
                try:
                    self.queue.put_nowait((file_obj, self.path_type))
                    self.sender.processed_files.add(file_key)
                except asyncio.QueueFull:
                    # Queue full, skip this file to prevent blocking
                    pass
        except Exception:
            pass

class SenderClient:

    def __init__(self, base_url: str, host: str, session_id: str = "sender"):
        self.base_url = base_url
        self.host = host
        self.session_id = session_id
        self.uuid = None
        self.ws = None
        self.use_websocket = True
        self.consecutive_failures = 0  # Consecutive failure count
        self.last_failure_time = 0     # Last failure timestamp
        self.last_successful_time = 0  # Track successful connections
        self.connection_health_score = 100  # Connection health metric

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.ws:
            await self.ws.close()

    async def authenticate(self) -> bool:
        import time

        current_time = time.time()
        if self.consecutive_failures > 0:
            backoff_time = min(5, 1.5 ** self.consecutive_failures)  # Max 5 seconds, faster recovery
            if current_time - self.last_failure_time < backoff_time:
                return False  # Still in backoff period

        hello_msg = {"type": "hello", "host": self.host, "session_id": self.session_id}

        # Build WS URL
        try:
            parts = self.base_url.split("//", 1)[-1]
            host_port = parts
            if "/" in parts:
                host_port = parts.split("/", 1)[0]
            ws_url = f"ws://{host_port}/ws"
        except Exception:
            ws_url = self.base_url.replace("http://", "ws://") + "/ws"

        for attempt in range(1, 4):  # 3 attempts for better success rate
            try:
                print(f"[DEBUG] Attempt {attempt}: Connecting to {ws_url}")
                self.ws = await websockets.connect(
                    ws_url,
                    ping_interval=20,      # Faster ping for quicker detection
                    open_timeout=5,        # Reduced timeout for faster failure detection
                    close_timeout=3
                )
                print(f"[DEBUG] WebSocket connected, sending hello...")
                await self.ws.send(json.dumps(hello_msg))

                try:
                    hello_ack_raw = await asyncio.wait_for(self.ws.recv(), timeout=5)  # Faster response timeout
                except asyncio.TimeoutError:
                    await self.ws.close()
                    self.ws = None
                    continue

                hello_ack = json.loads(hello_ack_raw) if isinstance(hello_ack_raw, str) else {}
                if hello_ack.get("accepted"):
                    self.uuid = hello_ack.get("uuid")
                    self.use_websocket = True
                    self.consecutive_failures = 0  # Reset failure count
                    self.last_successful_time = current_time
                    self.connection_health_score = min(100, self.connection_health_score + 10)
                    return True
                else:
                    await self.ws.close()
                    self.ws = None
                    await asyncio.sleep(0.5)
                    continue

            except Exception as e:
                if attempt == 1:
                    pass
                await asyncio.sleep(0.2)  # Faster retry

        self.consecutive_failures += 1
        self.last_failure_time = current_time
        self.connection_health_score = max(0, self.connection_health_score - 20)
        return False


    async def send_files(self, items: List[Dict[str, str]]) -> bool:
        if not (self.ws and self.uuid):
            return False
        batch_id = current_timestamp_ms()
        payload = {"type": "files", "uuid": self.uuid, "host": self.host, "items": items, "batch_id": batch_id}
        try:
            await self.ws.send(json.dumps(payload))
            # Drain acks with timeout to avoid hanging
            timeout_count = 0
            while timeout_count < 3:  # Max 3 timeout retries
                try:
                    raw = await asyncio.wait_for(self.ws.recv(), timeout=5)
                    ack = json.loads(raw) if isinstance(raw, str) else {}
                    if ack.get("type") == "files_ack" and (ack.get("batch_id") in (None, batch_id)):
                        return bool(ack.get("ok"))
                except asyncio.TimeoutError:
                    timeout_count += 1
                    continue
        except Exception:
            return False

    async def send_files_with_id(self, items: List[Dict[str, str]], batch_id: int) -> bool:
        if not (self.ws and self.uuid):
            return False
        payload = {"type": "files", "uuid": self.uuid, "host": self.host, "items": items, "batch_id": batch_id}
        try:
            await self.ws.send(json.dumps(payload))
            return True
        except Exception:
            return False

    @staticmethod
    def _is_sendable_file(file_path: Path) -> bool:
        return (file_path.exists() and
                file_path.name.endswith(".json") and
                "_SENT" not in file_path.name)

class FileSender:

    def __init__(self, config: Dict):
        self.config = config
        self.base_url = f"http://{config['host']}:{config.get('port', 444)}"
        self.local_host = config.get("edge_host", "localhost")
        self.service_name = config.get("service_name", "default_service")
        # 絕對路徑：不依賴工作目錄的垃圾設計
        self.wb_path = Path(config.get("wb_path", "./frame/")).resolve()
        self.grpc_path = Path(config.get("grpc_path", "./extracted_data/")).resolve()

        self.extreme_performance = config.get("extreme_performance", True)  # Default to high performance
        self.measurement_tool_mode = config.get("measurement_tool_mode", False)
        self.connection_stable_mode = config.get("connection_stable_mode", True)  # Default to stable

        if self.measurement_tool_mode:
            print("MEASUREMENT TOOL MODE: Optimized for real-time data transmission")
            grpc_files = list(self.grpc_path.glob("*.json")) if self.grpc_path.exists() else []
            unsent = len([f for f in grpc_files if "_SENT" not in f.name])
            if unsent > 0:
                print(f"Backlog detected: {unsent} files pending transmission")
        elif not self.extreme_performance:
            print(f"WB Path: {self.wb_path} (exists: {self.wb_path.exists()})")
            print(f"gRPC Path: {self.grpc_path} (exists: {self.grpc_path.exists()})")

            if self.wb_path.exists():
                wb_files = list(self.wb_path.glob("*.json"))
                wb_unsent = len([f for f in wb_files if not f.name.endswith("_SENT.json") and "_SENT" not in f.stem])
                print(f"WB: {len(wb_files)} JSON files, {wb_unsent} unsent")

            if self.grpc_path.exists():
                grpc_files = list(self.grpc_path.glob("*.json"))
                grpc_unsent = len([f for f in grpc_files if not f.name.endswith("_SENT.json") and "_SENT" not in f.stem])
                print(f"gRPC: {len(grpc_files)} JSON files, {grpc_unsent} unsent")
        else:
            print("EXTREME PERFORMANCE MODE: Minimal logging")

        self.stats_lock = asyncio.Lock()
        self.sent_count = 0
        self.error_count = 0
        self.last_stats_time = time.time()

        self.processed_files = set()
        self.processed_lock = asyncio.Lock()  # Prevent duplicate processing
        self.ws_batch_size = int(config.get("ws_batch_size", 500))  # Increased batch size
        self.ws_flush_ms = float(config.get("ws_flush_ms", 0.1)) / 1000.0  # Faster flush
        self.delete_sent_files = bool(config.get("delete_sent_files", True))  # Default to delete
        self.ws_connections = max(1, int(config.get("ws_connections", 8)))  # More connections

        print(f"Delete mode: {self.delete_sent_files}")

    async def _async_delete_file(self, path: Path, retries: int = 2) -> bool:
        """簡化的檔案刪除函數，專注於速度和可靠性"""
        for attempt in range(retries):
            try:
                if not path.exists():
                    return True
                
                path.unlink(missing_ok=True)
                return True
                
            except FileNotFoundError:
                return True  # Already deleted
            except Exception as e:
                if attempt == retries - 1 and not self.extreme_performance:
                    print(f"Delete failed: {path.name} - {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(0.01)  # Very short delay
        return False

    async def _cleanup_sent_files(self) -> None:
        """清理所有現存的 _SENT 檔案"""
        wb_sent = FileManager.find_sent_files(self.wb_path)
        grpc_sent = FileManager.find_sent_files(self.grpc_path)

        total_sent = len(wb_sent) + len(grpc_sent)
        if total_sent > 0:
            print(f"Cleanup: Found {len(wb_sent)} WB + {len(grpc_sent)} gRPC _SENT files")

            deleted = 0
            failed = 0

            # 清理 WB _SENT 檔案
            for sent_file in wb_sent:
                if await self._async_delete_file(sent_file):
                    deleted += 1
                else:
                    failed += 1

            # 清理 gRPC _SENT 檔案
            for sent_file in grpc_sent:
                if await self._async_delete_file(sent_file):
                    deleted += 1
                else:
                    failed += 1

            print(f"Cleanup complete: {deleted} deleted, {failed} failed")
        else:
            print("No _SENT files found for cleanup")


    async def run(self) -> None:
        # Setup file monitoring with bounded queue to prevent memory issues
        loop = asyncio.get_running_loop()
        max_queue_size = self.config.get("max_queue_size", 50000)  # Allow more files in queue
        queue = asyncio.Queue(maxsize=max_queue_size)

        # Clean up existing _SENT files if delete mode is enabled
        if self.delete_sent_files:
            await self._cleanup_sent_files()

        # Seed existing files
        await self._seed_existing_files(queue)
        print(f"Queue size after seeding: {queue.qsize()}")

        # Setup file watchers
        observer = self._setup_file_watchers(loop, queue)
        observer.start()

        # 多連線模式：每條連線獨立認證，各自處理批次
        workers: List[asyncio.Task] = []
        print(f"Using {self.ws_connections} WebSocket connections with parallel batching")
        for i in range(self.ws_connections):
            workers.append(asyncio.create_task(self._ws_connection_worker(queue, i)))

        # Start ultra-fast scanner (FIXED: 移出迴圈)
        scanners = []
        ultra_scanner = asyncio.create_task(self._ultra_fast_scanner(queue))
        scanners.append(ultra_scanner)
        print("Started ultra-fast scanner (<=1ms interval)")

        performance_monitor = asyncio.create_task(self._performance_monitor())
        queue_monitor = asyncio.create_task(self._queue_monitor(queue))

        try:
            await asyncio.Future()  # Run forever
        finally:
            observer.stop()
            observer.join(timeout=5)
            for worker in workers:
                worker.cancel()
            # 各 worker 會自行管理連線生命週期
            for scanner in scanners:
                scanner.cancel()
            performance_monitor.cancel()
            queue_monitor.cancel()

    async def _seed_existing_files(self, queue: asyncio.Queue) -> None:
        wb_files = FileManager.find_unsent_files(self.wb_path)
        grpc_files = FileManager.find_unsent_files(self.grpc_path)

        # Debug: 顯示所有檔案
        print(f"[DEBUG] WB config path: {self.config.get('wb_path')}")
        print(f"[DEBUG] WB resolved path: {self.wb_path}")
        print(f"[DEBUG] WB path exists: {self.wb_path.exists()}")
        print(f"[DEBUG] gRPC config path: {self.config.get('grpc_path')}")
        print(f"[DEBUG] gRPC resolved path: {self.grpc_path}")
        print(f"[DEBUG] gRPC path exists: {self.grpc_path.exists()}")

        if self.wb_path.exists():
            all_wb = list(self.wb_path.glob("*.json"))
            print(f"[DEBUG] All WB JSON files: {len(all_wb)}")
            for f in all_wb[:5]:  # 顯示前5個檔案
                print(f"[DEBUG] WB file: {f.name}")

        if self.grpc_path.exists():
            all_grpc = list(self.grpc_path.glob("*.json"))
            print(f"[DEBUG] All gRPC JSON files: {len(all_grpc)}")
            for f in all_grpc[:5]:  # 顯示前5個檔案
                print(f"[DEBUG] gRPC file: {f.name}")

        # 特別處理 t2 檔案（已加時間戳記但傳送失敗的檔案）
        wb_t2 = FileManager.find_t2_files(self.wb_path)
        grpc_t2 = FileManager.find_t2_files(self.grpc_path)

        print(f"[DEBUG] Filtered unsent files: {len(wb_files)} WB, {len(grpc_files)} gRPC")
        if not self.extreme_performance:
            print(f"Seeding queue: {len(wb_files)} WB files, {len(grpc_files)} gRPC files")
            if wb_t2 or grpc_t2:
                print(f"Found unsent t2 files: {len(wb_t2)} WB, {len(grpc_t2)} gRPC (will retry)")

        # 加入普通檔案
        for file_path in wb_files:
            await queue.put((file_path, "wb"))
        for file_path in grpc_files:
            await queue.put((file_path, "grpc"))

        # 加入 t2 檔案重傳
        for file_path in wb_t2:
            await queue.put((file_path, "wb"))
        for file_path in grpc_t2:
            await queue.put((file_path, "grpc"))

        total_files = len(wb_files) + len(grpc_files) + len(wb_t2) + len(grpc_t2)
        if not self.extreme_performance:
            print(f"Queue seeded with {total_files} files ({len(wb_t2) + len(grpc_t2)} t2 retries)")

    def _setup_file_watchers(self, loop: asyncio.AbstractEventLoop,
                           queue: asyncio.Queue) -> Observer:
        observer = Observer()

        wb_handler = FileEventHandler(loop, queue, "wb", self)
        grpc_handler = FileEventHandler(loop, queue, "grpc", self)

        if self.wb_path.exists():
            observer.schedule(wb_handler, str(self.wb_path), recursive=False)
        if self.grpc_path.exists():
            observer.schedule(grpc_handler, str(self.grpc_path), recursive=False)

        return observer




    async def _ultra_fast_scanner(self, queue: asyncio.Queue) -> None:
        scan_interval = self.config.get("scan_interval", 0.0001)  # 0.1ms for ultra-fast detection

        while True:
            try:
                await asyncio.sleep(scan_interval)

                # 優先掃描WebSocket目錄（檔案產生頻率更高）
                await self._scan_directory(queue, self.wb_path, "wb")
                await self._scan_directory(queue, self.grpc_path, "grpc")

            except Exception:
                pass

    async def _scan_directory(self, queue: asyncio.Queue, path: Path, path_type: str) -> None:
        try:
            files_to_queue = []
            for file_path in FileManager.find_unsent_files(path):
                if not file_path.exists():
                    continue

                file_key = str(file_path.absolute())

                # Thread-safe duplicate check
                async with self.processed_lock:
                    if file_key not in self.processed_files:
                        files_to_queue.append((file_path, file_key))
                        # Limit batch size per scan
                        if len(files_to_queue) >= 100:
                            break

            # Queue files efficiently
            for file_path, file_key in files_to_queue:
                try:
                    queue.put_nowait((file_path, path_type))
                    async with self.processed_lock:
                        self.processed_files.add(file_key)
                    if not (self.extreme_performance or self.measurement_tool_mode):
                        print(f"Queued: {file_path.name} ({path_type})")
                except asyncio.QueueFull:
                    # If queue full, use blocking put to ensure file gets processed
                    await queue.put((file_path, path_type))
                    async with self.processed_lock:
                        self.processed_files.add(file_key)

        except Exception:
            pass

    async def _performance_monitor(self) -> None:
        while True:
            try:
                await asyncio.sleep(30)  # Report every 30 seconds

                async with self.stats_lock:
                    current_time = time.time()
                    time_elapsed = current_time - self.last_stats_time

                    if time_elapsed > 0:
                        send_rate = self.sent_count / time_elapsed
                        error_rate = self.error_count / time_elapsed
                        success_percentage = (
                            self.sent_count / max(1, self.sent_count + self.error_count) * 100
                        )

                        print(f"Performance: {send_rate:.1f} files/sec, "
                              f"{success_percentage:.1f}% success rate, "
                              f"{error_rate:.2f} errors/sec")

                        # Reset counters
                        self.sent_count = 0
                        self.error_count = 0
                        self.last_stats_time = current_time

            except Exception:
                pass

    async def _queue_monitor(self, queue: asyncio.Queue) -> None:
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute

                queue_size = queue.qsize()
                processed_count = len(self.processed_files)

                if not self.extreme_performance:
                    print(f"Queue monitor: {queue_size} pending, {processed_count} processed")

                # Clean up old processed files to prevent memory leak
                if processed_count > 50000:
                    async with self.processed_lock:
                        # Keep only recent 25000 entries
                        recent_files = set(list(self.processed_files)[-25000:])
                        self.processed_files = recent_files
                        print(f"Cleaned processed files cache: {processed_count} -> {len(recent_files)}")

                # Alert if queue is getting too full
                if queue_size > queue.maxsize * 0.9:
                    print(f"WARNING: Queue nearly full ({queue_size}/{queue.maxsize}), processing may slow")

            except Exception:
                pass



    async def _ws_connection_worker(self, queue: asyncio.Queue, worker_id: int) -> None:
        print(f"[DEBUG] Starting worker {worker_id}")
        client = None
        batch: List[Tuple[Path, str]] = []
        sent_count = 0
        last_stats = time.time()

        while True:
            try:
                # Get file from queue - blocking wait for efficiency
                file_path, path_type = await queue.get()
                print(f"[DEBUG] Worker {worker_id} got file: {file_path.name} (type: {path_type})")

                # 確保連線存在，使用智能重連，根據服務名稱使用session_id
                if not client:
                    session_id = f"{self.service_name}_{path_type}"  # Combine service name with data type
                    print(f"[DEBUG] Worker {worker_id} creating client with session_id: {session_id}")
                    client = SenderClient(self.base_url, self.local_host, session_id)
                    print(f"[DEBUG] Worker {worker_id} attempting authentication...")
                    auth_success = await client.authenticate()
                    print(f"[DEBUG] Worker {worker_id} authentication result: {auth_success}")
                    if not auth_success:
                        # 連線穩定模式：更長的等待時間
                        # Adaptive sleep based on connection health
                        base_sleep = 2 if self.connection_stable_mode else 0.5
                        health_factor = (100 - self.connection_health_score) / 100
                        sleep_time = base_sleep + (3 * health_factor)  # 0.5-5.5s range
                        await asyncio.sleep(sleep_time)
                        continue

                    if self.measurement_tool_mode and client.consecutive_failures == 0:
                        print(f"Worker[{worker_id}] connected, uuid={client.uuid}")
                    elif not self.extreme_performance:
                        print(f"Worker[{worker_id}] authenticated, uuid={client.uuid}")

                # Add file to batch - removed duplicate queue.get()
                batch.append((file_path, path_type))

                if not self.extreme_performance:
                    print(f"Worker[{worker_id}] processing: {file_path.name}")

                # 有檔案就送，不管多少個
                if batch:
                    # 構建 payload（讀檔）- 追蹤重命名的檔案路徑
                    items: List[Dict[str, str]] = []
                    renamed_files: List[Path] = []  # 追蹤實際的檔案路徑

                    for file_path, path_type in batch:
                        try:
                            # Simple existence check
                            if not file_path.exists() or not SenderClient._is_sendable_file(file_path):
                                continue

                            # Add timestamp if needed
                            if FilenameProcessor.has_two_timestamps(file_path.stem):
                                fp2 = file_path
                            else:
                                fp2 = FilenameProcessor.add_send_timestamp(file_path)
                                if not fp2.exists():
                                    continue

                            # Async file read
                            content = await asyncio.to_thread(fp2.read_text, encoding="utf-8")
                            items.append({
                                "path_type": path_type,
                                "filename": fp2.name,
                                "content": content,
                            })
                            renamed_files.append(fp2)
                        except Exception:
                            continue

                    if items:
                        batch_id = current_timestamp_ms()
                        print(f"[DEBUG] Worker {worker_id} sending {len(items)} files")
                        sent = await client.send_files_with_id(items, batch_id)
                        print(f"[DEBUG] Worker {worker_id} send result: {sent}")

                        # Simple retry on failure
                        if not sent and await client.authenticate():
                            sent = await client.send_files_with_id(items, batch_id)
                        if sent:
                            sent_count += len(items)
                            
                            # 立即刪除傳送成功的檔案
                            delete_success = 0
                            delete_fail = 0
                            
                            for renamed_file in renamed_files:
                                if self.delete_sent_files:
                                    if await self._async_delete_file(renamed_file, retries=1):
                                        delete_success += 1
                                    else:
                                        delete_fail += 1
                                else:
                                    FileManager.mark_as_sent(renamed_file)

                            # Clean processed_files records
                            async with self.processed_lock:
                                for orig_path, _ in batch:
                                    orig_key = str(orig_path.absolute())
                                    self.processed_files.discard(orig_key)

                            # 簡化報告
                            if not self.extreme_performance:
                                print(f"Worker[{worker_id}]: sent {len(items)}, deleted {delete_success}, failed {delete_fail}")
                        else:
                            # Send failed, requeue files
                            print(f"Worker[{worker_id}] Send failed, requeueing {len(renamed_files)} files")
                            for renamed_file in renamed_files:
                                path_type = "grpc" if self.grpc_path in renamed_file.parents else "wb"
                                await queue.put((renamed_file, path_type))
                    batch.clear()

                # 測量工具模式：減少統計輸出頻率
                now = time.time()
                if self.measurement_tool_mode:
                    report_interval = 60  # 測量工具模式：每分鐘報告
                elif self.extreme_performance:
                    report_interval = 30  # 極致性能：每30秒
                else:
                    report_interval = 5   # 正常模式：每5秒

                if now - last_stats >= report_interval:
                    rate = sent_count / (now - last_stats) if now > last_stats else 0
                    if self.measurement_tool_mode:
                        print(f"Worker[{worker_id}]: {rate:.0f} JSON/s")
                    else:
                        print(f"Worker[{worker_id}] rate: {rate:.1f} JSON/s, total: {sent_count}")
                    sent_count = 0
                    last_stats = now
            except Exception:
                # Reset client on any error
                if client:
                    try:
                        await client.__aexit__(None, None, None)
                    except Exception:
                        pass
                    client = None
                await asyncio.sleep(0.5)
            finally:
                # Mark queue tasks as done
                for _ in range(len(batch) if batch else 1):  # At least 1 for the file we got
                    try:
                        queue.task_done()
                    except Exception:
                        pass


async def main():
    config = load_config()
    sender = FileSender(config)
    await sender.run()


if __name__ == "__main__":
    asyncio.run(main())