#!/usr/bin/env python3
"""
Base monitoring framework for packet capture and JSON extraction.
Eliminates code duplication between WebSocket and gRPC monitors.
"""

import json
import os
import platform
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any


class TsharkLocator:
    """Finds tshark executable across platforms."""

    @staticmethod
    def get_tshark_path() -> str:
        if platform.system() == 'Windows':
            possible_paths = [
                "C:\\Program Files\\Wireshark\\tshark.exe",
                "C:\\Program Files (x86)\\Wireshark\\tshark.exe",
                "C:\\Program Files\\Wireshark\\bin\\tshark.exe",
                "C:\\Program Files (x86)\\Wireshark\\bin\\tshark.exe",
                "tshark"
            ]

            for path in possible_paths:
                if os.path.exists(path) or path == "tshark":
                    return path
        else:
            # Linux/Mac
            return "tshark"

        return "tshark"


class NetworkInterface:
    """Manages network interface detection."""

    @staticmethod
    def get_local_ips() -> List[str]:
        try:
            hostname = socket.gethostname()
            return [socket.gethostbyname(hostname)]
        except Exception:
            return ["127.0.0.1"]

    @staticmethod
    def detect_interface() -> Optional[str]:
        """Detect appropriate network interface."""
        # This is simplified - real implementation would detect based on active connections
        return None


class JSONExtractor(ABC):
    """Abstract base for JSON content extraction from packets."""

    def __init__(self):
        self.frame_data_pattern = re.compile(r'^\d+$')

    @abstractmethod
    def extract_json_from_packet(self, packet_data: str) -> Optional[Dict[str, Any]]:
        """Extract JSON data from packet."""
        pass

    @abstractmethod
    def should_save_json(self, json_data: Dict[str, Any]) -> bool:
        """Determine if JSON data should be saved."""
        pass

    @abstractmethod
    def get_frame_type(self, json_data: Dict[str, Any]) -> str:
        """Determine frame type (2D/3D) from JSON data."""
        pass


class WebSocketJSONExtractor(JSONExtractor):
    """JSON extractor for WebSocket traffic."""

    def extract_json_from_packet(self, packet_data: str) -> Optional[Dict[str, Any]]:
        try:
            # Handle hex payload from tshark -T fields -e tcp.payload
            if packet_data.strip():
                # Try to decode hex data first
                hex_data = packet_data.strip().replace(':', '').replace(' ', '')
                if all(c in '0123456789abcdefABCDEF' for c in hex_data) and len(hex_data) >= 4:
                    try:
                        decoded = bytes.fromhex(hex_data).decode('utf-8', errors='ignore')
                        # Look for JSON in decoded data
                        json_data = self._extract_json_from_text(decoded)
                        if json_data:
                            return json_data
                    except:
                        pass

                # Also try raw text (in case it's not hex)
                json_data = self._extract_json_from_text(packet_data)
                if json_data:
                    return json_data

        except Exception:
            pass
        return None

    def _extract_json_from_text(self, text: str) -> Optional[Dict[str, Any]]:
        """Extract JSON (object or array) from text using balanced delimiter counting"""
        try:
            # 找到可能的 JSON 起始符號，支援物件 { 與 陣列 [
            obj_start = text.find('{')
            arr_start = text.find('[')

            # 沒有任何 JSON 起始符
            if obj_start == -1 and arr_start == -1:
                return None

            # 選擇更靠前的起始位置與類型
            if arr_start != -1 and (obj_start == -1 or arr_start < obj_start):
                start_idx = arr_start
                open_ch, close_ch = '[', ']'
            else:
                start_idx = obj_start
                open_ch, close_ch = '{', '}'

            depth = 0
            end_idx = start_idx
            for i in range(start_idx, len(text)):
                ch = text[i]
                if ch == open_ch:
                    depth += 1
                elif ch == close_ch:
                    depth -= 1
                    if depth == 0:
                        end_idx = i
                        break

            if depth == 0:
                json_str = text[start_idx:end_idx + 1]
                try:
                    return json.loads(json_str)
                except:
                    pass

        except Exception:
            pass
        return None

    def should_save_json(self, json_data: Dict[str, Any]) -> bool:
        """WebSocket filtering: only save JSON that contains concrete coordinates.

        Rules:
        - 必須含有實際座標資料，例如 x,y 或 x,y,z（數值型），或是座標點陣列
        - 嚴格排除所有2D相關數據：bbox、keypoints、id等
        - 其他全部不要儲存
        """
        if not json_data or len(json_data) == 0:
            return False

        # 嚴格排除所有2D相關結構
        if self._is_2d_data(json_data):
            return False

        return self._has_coordinates(json_data)

    def _is_2d_data(self, data: Dict[str, Any]) -> bool:
        """嚴格檢查是否為2D數據，包含任何2D相關字段都排除"""
        # 2D檢測關鍵字
        detection_keys = [
            "bbox", "bounding_box", "boundingBox", "box", "bounds", "rectangle",
            "keypoints", "keypoint", "landmarks", "landmark",
            "id", "ID", "identifier", "object_id", "track_id", "entity_id",
            "confidence", "score", "probability"
        ]

        # 檢查頂層字段
        for key in data.keys():
            if key.lower() in [k.lower() for k in detection_keys]:
                return True

        # 如果有keypoints陣列結構（2D pose detection的典型特徵）
        if "keypoints" in data and isinstance(data["keypoints"], list):
            return True

        # 如果有id+座標組合（2D detection的典型特徵）
        if any(k in data for k in ["id", "ID"]):
            # 檢查是否有2D座標陣列
            for value in data.values():
                if isinstance(value, list) and len(value) > 0:
                    first_item = value[0]
                    if isinstance(first_item, list) and len(first_item) >= 2:
                        # 檢查是否為2D點格式 [x, y, confidence]
                        if all(isinstance(x, (int, float)) for x in first_item[:3]):
                            return True

        # 檢查嵌套結構
        for value in data.values():
            if isinstance(value, dict):
                if self._is_2d_data(value):
                    return True
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and self._is_2d_data(item):
                        return True

        return False

    def _has_bbox_structure(self, data: Dict[str, Any]) -> bool:
        """檢查是否包含bbox相關結構，如果有就應該由gRPC monitor處理。"""
        bbox_keys = ["bbox", "bounding_box", "boundingBox", "box", "bounds", "rectangle"]

        # 檢查頂層
        if any(key in data for key in bbox_keys):
            return True

        # 檢查嵌套結構
        for value in data.values():
            if isinstance(value, dict):
                if any(key in value for key in bbox_keys):
                    return True
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and any(key in item for key in bbox_keys):
                        return True

        return False

    def _has_coordinates(self, data: Any) -> bool:
        """Recursively check for concrete coordinate data.
        Accepts:
        - Dict with numeric x,y or x,y,z
        - List of dicts each having numeric x,y(,z)
        - Nested structures under common keys like position/coordinate/points
        - Pure numeric lists representing points, e.g. [x, y], [x, y, z], or [x, y, z, -1]
        """
        def is_number(value: Any) -> bool:
            return isinstance(value, (int, float)) and not isinstance(value, bool)

        def is_numeric_point_list(value: Any) -> bool:
            # Accept [x, y], [x, y, z], or [x, y, z, visibility]
            if isinstance(value, list) and len(value) >= 2:
                # 前兩個必須是數值（x,y）
                if not (is_number(value[0]) and is_number(value[1])):
                    return False
                # 其餘元素若存在，可為數值（例如 z 或可見度）
                for elem in value[2:]:
                    if not is_number(elem):
                        return False
                return True
            return False

        if isinstance(data, dict):
            keys_lower = {k.lower(): k for k in data.keys()}
            # Direct x,y,(z)
            if 'x' in keys_lower and 'y' in keys_lower:
                x_key = keys_lower['x']
                y_key = keys_lower['y']
                z_key = keys_lower.get('z')
                if is_number(data.get(x_key)) and is_number(data.get(y_key)):
                    if z_key is None or is_number(data.get(z_key)):
                        return True
            # Common containers
            for container_key in ['position', 'coordinate', 'coordinates', 'location', 'point', 'points']:
                if container_key in keys_lower:
                    inner = data.get(keys_lower[container_key])
                    if self._has_coordinates(inner):
                        return True
            # Recurse values (also handle numeric list containers under arbitrary keys like "1")
            for v in data.values():
                # 如果值本身是數值點陣列，直接判定為座標
                if is_numeric_point_list(v):
                    return True
                if isinstance(v, (dict, list)):
                    if self._has_coordinates(v):
                        return True
        elif isinstance(data, list):
            # Case 1: 這個 list 本身就是一個座標點 [x,y(,z...)]
            if is_numeric_point_list(data):
                return True
            # Case 2: list 內包含多個點或更深層結構
            for item in data:
                if is_numeric_point_list(item):
                    return True
                if isinstance(item, (dict, list)):
                    if self._has_coordinates(item):
                        return True
        return False

    def get_frame_type(self, json_data: Dict[str, Any]) -> str:
        # WebSocket: always 3D prefix
        return "3D"


class GRPCJSONExtractor(JSONExtractor):
    """JSON extractor for gRPC traffic."""

    def extract_json_from_packet(self, packet_data: str) -> Optional[Dict[str, Any]]:
        try:
            # Handle hex payload from tshark -T fields -e tcp.payload
            if packet_data.strip():
                # Try to decode hex data first
                hex_data = packet_data.strip().replace(':', '').replace(' ', '')
                if all(c in '0123456789abcdefABCDEF' for c in hex_data) and len(hex_data) >= 4:
                    try:
                        decoded = bytes.fromhex(hex_data).decode('utf-8', errors='ignore')
                        # Look for JSON in decoded data
                        json_data = self._extract_json_from_text(decoded)
                        if json_data:
                            return json_data
                    except:
                        pass

                # Also try raw text (in case it's not hex)
                json_data = self._extract_json_from_text(packet_data)
                if json_data:
                    return json_data

        except Exception:
            pass
        return None

    def _extract_json_from_text(self, text: str) -> Optional[Dict[str, Any]]:
        """Extract JSON from text using balanced brace counting"""
        try:
            start_idx = text.find('{')
            if start_idx == -1:
                return None

            brace_count = 0
            end_idx = start_idx

            for i in range(start_idx, len(text)):
                if text[i] == '{':
                    brace_count += 1
                elif text[i] == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_idx = i
                        break

            if brace_count == 0:  # Found complete JSON
                json_str = text[start_idx:end_idx + 1]
                try:
                    return json.loads(json_str)
                except:
                    pass

        except Exception:
            pass
        return None

    def should_save_json(self, json_data: Dict[str, Any]) -> bool:
        """gRPC filtering: only save JSON with id and bbox fields."""
        if not json_data or len(json_data) == 0:
            return False

        # Must contain both id and bbox
        return self._has_id_and_bbox(json_data)

    def _has_id_and_bbox(self, data: Dict[str, Any]) -> bool:
        """Check if gRPC data has required id and bbox fields."""
        # Check for id field (various forms)
        has_id = any(field in data for field in [
            "id", "ID", "identifier", "object_id", "track_id", "entity_id"
        ])

        # Check for bbox field (various forms)
        has_bbox = any(field in data for field in [
            "bbox", "bounding_box", "boundingBox", "box", "bounds", "rectangle"
        ])

        # Also check nested structures
        if not (has_id and has_bbox):
            for value in data.values():
                if isinstance(value, dict):
                    if self._has_id_and_bbox(value):
                        return True
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict) and self._has_id_and_bbox(item):
                            return True

        return has_id and has_bbox

    def get_frame_type(self, json_data: Dict[str, Any]) -> str:
        # gRPC: always 2D prefix (changed from misc)
        return "2D"


class PacketMonitor:
    """Base packet monitoring framework."""

    def __init__(self, config_path: str, json_extractor: JSONExtractor):
        self.config_path = config_path
        self.json_extractor = json_extractor
        self.config = {}
        self.debug_mode = True

        # Monitoring state
        self.monitoring_active = False
        self.monitoring_start_time = None
        self.stop_reason = "未啟動"

        # Paths and process
        self.tshark_path = TsharkLocator.get_tshark_path()
        self.tshark_process = None
        self.json_save_path = None

        # Statistics
        self.total_packets = 0
        self.total_messages = 0
        self.saved_frames = 0
        self.filtered_messages = 0  # Count of filtered messages
        self.stats_lock = threading.Lock()

        # Configuration
        self.interface = None
        self.monitor_interval = 1.0

        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup signal handlers for clean shutdown."""
        if platform.system() == 'Windows':
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        else:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        print(f"\\n收到信號 {signum}，正在停止監控...")
        self.stop_monitoring("收到停止信號")

    def load_config(self) -> bool:
        """Load configuration from file."""
        try:
            if not os.path.exists(self.config_path):
                print(f"配置檔案不存在: {self.config_path}")
                return False

            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)

            self.json_save_path = self.config.get('json_save_path', './extracted_data/')
            self.interface = self.config.get('interface')

            return True

        except Exception as e:
            print(f"載入配置檔案失敗: {e}")
            return False

    def start_monitoring(self) -> bool:
        """Start packet monitoring."""
        if not self.load_config():
            return False

        if not self._validate_tshark():
            return False

        # Ensure save directory exists
        Path(self.json_save_path).mkdir(parents=True, exist_ok=True)

        # Build tshark command
        cmd = self._build_tshark_command()
        if not cmd:
            return False

        if self.debug_mode:
            print(f"[DEBUG] 執行 tshark 命令: {' '.join(cmd)}")

        try:
            self.tshark_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1
            )

            self.monitoring_active = True
            self.monitoring_start_time = time.time()
            self.stop_reason = ""

            print(f"開始監控 - PID: {self.tshark_process.pid}")
            self._monitor_packets()

            return True

        except Exception as e:
            print(f"啟動監控失敗: {e}")
            return False

    def stop_monitoring(self, reason: str = "手動停止"):
        """Stop packet monitoring."""
        self.monitoring_active = False
        self.stop_reason = reason

        if self.tshark_process:
            try:
                self.tshark_process.terminate()
                self.tshark_process.wait(timeout=5)
            except Exception:
                try:
                    self.tshark_process.kill()
                except Exception:
                    pass

        self._print_final_stats()

    def _validate_tshark(self) -> bool:
        """Validate tshark is available."""
        try:
            result = subprocess.run(
                [self.tshark_path, '-v'],
                capture_output=True,
                text=True,
                timeout=10
            )
            return result.returncode == 0
        except Exception:
            print(f"無法執行 tshark: {self.tshark_path}")
            return False

    @abstractmethod
    def _build_tshark_command(self) -> Optional[List[str]]:
        """Build tshark command for specific protocol."""
        pass

    def _monitor_packets(self):
        """Monitor packets from tshark output."""
        try:
            for line in iter(self.tshark_process.stdout.readline, ''):
                if not self.monitoring_active:
                    break

                line = line.strip()
                if line:
                    self._process_packet_line(line)

        except Exception as e:
            if self.monitoring_active:
                print(f"監控過程發生錯誤: {e}")
                traceback.print_exc()

    def _process_packet_line(self, line: str):
        """Process a single packet line."""
        with self.stats_lock:
            self.total_packets += 1

        if self.debug_mode and line.strip():
            print(f"[DEBUG] 收到封包資料: {line[:100]}...")

        try:
            if self.debug_mode and line.strip():
                print(f"[DEBUG] 正在解析封包: {line[:100]}...")

            json_data = self.json_extractor.extract_json_from_packet(line)
            if json_data:
                with self.stats_lock:
                    self.total_messages += 1

                if self.debug_mode:
                    print(f"[DEBUG] ✓ 成功解析到 JSON!")
                    print(f"[DEBUG] JSON keys: {list(json_data.keys())}")
                    print(f"[DEBUG] JSON 內容: {str(json_data)[:300]}...")
                    print(f"[DEBUG] JSON 長度: {len(str(json_data))} 字符")

                should_save = self.json_extractor.should_save_json(json_data)
                if self.debug_mode:
                    print(f"[DEBUG] 儲存條件檢查: {should_save}")

                if should_save:
                    if self.debug_mode:
                        print(f"[DEBUG] → 開始儲存 JSON 檔案...")
                    self._save_json_data(json_data)
                    if self.debug_mode:
                        print(f"[DEBUG] ✓ JSON 檔案儲存完成")
                else:
                    with self.stats_lock:
                        self.filtered_messages += 1
                    if self.debug_mode:
                        print(f"[DEBUG] ✗ JSON 不符合儲存條件，跳過")
            else:
                if self.debug_mode and line.strip():
                    print(f"[DEBUG] ✗ 無法從封包提取 JSON")

        except Exception as e:
            if self.debug_mode:
                print(f"[ERROR] 處理封包時發生錯誤: {e}")
                print(f"[ERROR] 錯誤封包內容: {line[:200]}...")
                import traceback
                traceback.print_exc()

    def _save_json_data(self, json_data: Dict[str, Any]):
        """Save JSON data to file."""
        try:
            frame_type = self.json_extractor.get_frame_type(json_data)
            timestamp_ms = int(time.time() * 1000)

            filename = f"{frame_type}_{timestamp_ms}.json"
            filepath = Path(self.json_save_path) / filename

            if self.debug_mode:
                print(f"[DEBUG] 儲存路徑: {filepath}")
                print(f"[DEBUG] 檔案名稱: {filename}")
                print(f"[DEBUG] 確保目錄存在: {self.json_save_path}")

            # Ensure directory exists
            Path(self.json_save_path).mkdir(parents=True, exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)

            with self.stats_lock:
                self.saved_frames += 1

            if self.debug_mode:
                file_size = filepath.stat().st_size if filepath.exists() else 0
                print(f"[DEBUG] ✓ 檔案已儲存: {filename} ({file_size} bytes)")

        except Exception as e:
            print(f"[ERROR] 儲存 JSON 檔案失敗: {e}")
            import traceback
            traceback.print_exc()

    def _print_final_stats(self):
        """Print final monitoring statistics."""
        if self.monitoring_start_time:
            duration = time.time() - self.monitoring_start_time
            print(f"\\n監控統計:")
            print(f"  持續時間: {duration:.1f} 秒")
            print(f"  總封包數: {self.total_packets}")
            print(f"  總訊息數: {self.total_messages}")
            print(f"  儲存檔案: {self.saved_frames}")
            print(f"  過濾封包: {self.filtered_messages}")
            print(f"  停止原因: {self.stop_reason}")


class WebSocketMonitor(PacketMonitor):
    """WebSocket packet monitor."""

    def __init__(self, config_path: str = "./websocket_config.json"):
        super().__init__(config_path, WebSocketJSONExtractor())

    def _build_tshark_command(self) -> Optional[List[str]]:
        """Build tshark command for WebSocket monitoring."""
        cmd = [self.tshark_path]

        if self.interface:
            cmd.extend(['-i', self.interface])

        # WebSocket filter - monitor all ports
        cmd.extend([
            '-f', 'tcp',  # Capture all TCP traffic
            '-Y', 'tcp.payload',  # Any TCP packet with payload
            '-T', 'fields',
            '-e', 'tcp.payload',
            '-E', 'separator=|'
        ])

        return cmd


class GRPCMonitor(PacketMonitor):
    """gRPC packet monitor."""

    def __init__(self, config_path: str = "./grpc_config.json"):
        super().__init__(config_path, GRPCJSONExtractor())

    def _build_tshark_command(self) -> Optional[List[str]]:
        """Build tshark command for gRPC monitoring."""
        cmd = [self.tshark_path]

        if self.interface:
            cmd.extend(['-i', self.interface])

        # gRPC filter - monitor all ports
        cmd.extend([
            '-f', 'tcp',  # Capture all TCP traffic
            '-Y', 'tcp.payload',  # Any TCP packet with payload
            '-T', 'fields',
            '-e', 'tcp.payload',
            '-E', 'separator=|'
        ])

        return cmd