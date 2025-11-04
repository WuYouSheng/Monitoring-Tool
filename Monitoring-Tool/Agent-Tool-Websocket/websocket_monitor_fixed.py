#!/usr/bin/env python3
"""
WebSocket Monitor - Refactored
Clean implementation using unified monitoring framework.
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from monitor_base import WebSocketMonitor


def main():
    """Main entry point."""
    print("WebSocket 監控器 (重構版)")
    print("=" * 50)

    config_path = Path(__file__).with_name("websocket_config.json")
    monitor = WebSocketMonitor(str(config_path))

    try:
        if monitor.start_monitoring():
            print("監控已啟動，按 Ctrl+C 停止")
            # Monitor will run until interrupted
        else:
            print("啟動監控失敗")
    except KeyboardInterrupt:
        print("\\n收到停止信號")
        monitor.stop_monitoring("用戶中斷")
    except Exception as e:
        print(f"監控過程發生錯誤: {e}")
        monitor.stop_monitoring(f"錯誤: {e}")


if __name__ == "__main__":
    main()