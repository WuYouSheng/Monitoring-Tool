#!/usr/bin/env python3
"""
Common data structures and utilities for Agent-Tool QoS monitoring system.
Follows Linus's philosophy: good taste eliminates special cases.
"""

import time
import json
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from enum import Enum


class FrameType(Enum):
    FRAME_2D = "2D"
    FRAME_3D = "3D"
    MISC = "misc"


@dataclass
class TimestampInfo:
    ts1: Optional[int]  # sender intercept start
    ts2: Optional[int]  # sender before sending

    @classmethod
    def from_filename(cls, filename: str) -> 'TimestampInfo':
        """Parse timestamps from filename. Eliminates special cases."""
        base = Path(filename).stem
        parts = base.split("_")

        ts1 = cls._parse_timestamp(parts, 1)
        ts2 = cls._parse_timestamp(parts, 2)

        return cls(ts1=ts1, ts2=ts2)

    @staticmethod
    def _parse_timestamp(parts: List[str], index: int) -> Optional[int]:
        try:
            if len(parts) > index:
                return int(parts[index])
        except (ValueError, IndexError):
            pass
        return None


@dataclass
class FrameMetric:
    received_ms: int
    network_latency_ms: Optional[int]
    local_latency_ms: Optional[int]
    sender_local_latency_ms: Optional[int]
    is_valid_frame: bool
    frame_type: FrameType


@dataclass
class QoSStats:
    min_val: Optional[float]
    max_val: Optional[float]
    avg_val: Optional[float]

    @classmethod
    def from_values(cls, values: List[Union[int, float]]) -> 'QoSStats':
        if not values:
            return cls(None, None, None)

        float_vals = [float(v) for v in values]
        return cls(
            min_val=min(float_vals),
            max_val=max(float_vals),
            avg_val=sum(float_vals) / len(float_vals)
        )


def determine_frame_type(filename: str) -> FrameType:
    """Determine frame type from filename. No special cases."""
    if filename.startswith("3D_"):
        return FrameType.FRAME_3D
    elif filename.startswith("2D_"):
        return FrameType.FRAME_2D
    else:
        return FrameType.MISC


def validate_json_content(content: str) -> bool:
    """Validate JSON content. Simple boolean result."""
    try:
        parsed = json.loads(content) if isinstance(content, str) and content else {}
        return bool(parsed)
    except (json.JSONDecodeError, TypeError):
        return False


def current_timestamp_ms() -> int:
    """Current timestamp in milliseconds."""
    return int(time.time() * 1000)


def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert value to int."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert value to float."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default