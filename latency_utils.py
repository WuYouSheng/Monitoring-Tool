#!/usr/bin/env python3
"""
Enhanced Latency Calculation Utilities
Provides robust, production-ready latency calculation with comprehensive safeguards.
"""

import statistics
import time
from typing import List, Optional, Tuple, Dict
from dataclasses import dataclass
from enum import Enum


class LatencyIssue(Enum):
    NEGATIVE_LATENCY = "negative_latency"
    EXCESSIVE_LATENCY = "excessive_latency"
    CLOCK_DRIFT = "clock_drift"
    TIMESTAMP_ORDER_ERROR = "timestamp_order_error"
    OUTLIER_DETECTED = "outlier_detected"


@dataclass
class LatencyValidationResult:
    """Result of latency validation with potential issues flagged."""
    value: Optional[float]
    is_valid: bool
    issues: List[LatencyIssue]
    original_value: Optional[float] = None
    confidence_score: float = 1.0  # 0.0 to 1.0


class TimestampValidator:
    """Validates timestamps for reasonableness and consistency."""

    # More generous timestamp range: 1970-01-01 to 2070-01-01 (in milliseconds)
    MIN_REASONABLE_TIMESTAMP = 0  # Unix epoch
    MAX_REASONABLE_TIMESTAMP = 3155760000000  # Year 2070

    # Maximum reasonable latencies (in milliseconds)
    MAX_SENDER_LOCAL_LATENCY = 60000    # 1 minute
    MAX_NETWORK_LATENCY = 300000        # 5 minutes
    MAX_LOCAL_PROCESSING_LATENCY = 10000  # 10 seconds

    @classmethod
    def validate_timestamp(cls, timestamp: Optional[int]) -> bool:
        """Validate if timestamp is reasonable."""
        if timestamp is None:
            return False

        return cls.MIN_REASONABLE_TIMESTAMP <= timestamp <= cls.MAX_REASONABLE_TIMESTAMP

    @classmethod
    def validate_timestamp_pair(cls, ts1: Optional[int], ts2: Optional[int]) -> Tuple[bool, List[LatencyIssue]]:
        """Validate a pair of timestamps for consistency."""
        issues = []

        if ts1 is None or ts2 is None:
            return False, issues

        # Check individual timestamp validity
        if not cls.validate_timestamp(ts1):
            issues.append(LatencyIssue.TIMESTAMP_ORDER_ERROR)
        if not cls.validate_timestamp(ts2):
            issues.append(LatencyIssue.TIMESTAMP_ORDER_ERROR)

        # Check for excessive time difference (using absolute value)
        if abs(ts1 - ts2) > cls.MAX_SENDER_LOCAL_LATENCY:
            issues.append(LatencyIssue.EXCESSIVE_LATENCY)

        return len(issues) == 0, issues


class OutlierFilter:
    """Statistical outlier detection and filtering."""

    @staticmethod
    def detect_outliers_iqr(values: List[float], multiplier: float = 1.5) -> List[bool]:
        """Detect outliers using Interquartile Range method."""
        if len(values) < 4:
            return [False] * len(values)  # Not enough data for reliable detection

        q1 = statistics.quantiles(values, n=4)[0]  # 25th percentile
        q3 = statistics.quantiles(values, n=4)[2]  # 75th percentile
        iqr = q3 - q1

        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr

        return [v < lower_bound or v > upper_bound for v in values]

    @staticmethod
    def detect_outliers_zscore(values: List[float], threshold: float = 3.0) -> List[bool]:
        """Detect outliers using Z-score method."""
        if len(values) < 2:
            return [False] * len(values)

        mean_val = statistics.mean(values)

        if len(values) < 3:
            return [False] * len(values)  # Can't compute stdev with < 2 values

        std_val = statistics.stdev(values)
        if std_val == 0:
            return [False] * len(values)  # No variation

        z_scores = [(v - mean_val) / std_val for v in values]
        return [abs(z) > threshold for z in z_scores]

    @classmethod
    def filter_outliers(cls, values: List[float], method: str = "iqr") -> Tuple[List[float], List[int]]:
        """Filter outliers and return cleaned values with removed indices."""
        if len(values) < 4:
            return values, []  # Not enough data for reliable filtering

        if method == "iqr":
            outliers = cls.detect_outliers_iqr(values)
        elif method == "zscore":
            outliers = cls.detect_outliers_zscore(values)
        else:
            raise ValueError(f"Unknown outlier detection method: {method}")

        filtered_values = [v for i, v in enumerate(values) if not outliers[i]]
        removed_indices = [i for i, is_outlier in enumerate(outliers) if is_outlier]

        return filtered_values, removed_indices


class RobustLatencyCalculator:
    """Production-ready latency calculator with comprehensive safeguards."""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.clock_drift_tolerance = self.config.get("clock_drift_tolerance_ms", 5000)  # 5 seconds
        self.enable_outlier_filtering = self.config.get("enable_outlier_filtering", True)
        self.outlier_method = self.config.get("outlier_method", "iqr")

        # Keep track of baseline for clock drift correction
        self.baseline_corrections: Dict[str, int] = {}

    def calculate_sender_local_latency(self, ts1: Optional[int], ts2: Optional[int]) -> LatencyValidationResult:
        """Calculate sender local latency with validation."""
        issues = []

        if ts1 is None or ts2 is None:
            return LatencyValidationResult(None, False, [])

        # Validate timestamp pair
        is_valid_pair, pair_issues = TimestampValidator.validate_timestamp_pair(ts1, ts2)
        issues.extend(pair_issues)

        if not is_valid_pair:
            return LatencyValidationResult(None, False, issues)

        raw_latency = abs(ts1 - ts2)

        # Apply safeguards
        if raw_latency < 0:
            issues.append(LatencyIssue.NEGATIVE_LATENCY)
            corrected_latency = 0.0
        elif raw_latency > TimestampValidator.MAX_SENDER_LOCAL_LATENCY:
            issues.append(LatencyIssue.EXCESSIVE_LATENCY)
            corrected_latency = float(TimestampValidator.MAX_SENDER_LOCAL_LATENCY)
        else:
            corrected_latency = float(raw_latency)

        # Calculate confidence score
        confidence = 1.0
        if issues:
            confidence = max(0.1, 1.0 - len(issues) * 0.3)

        return LatencyValidationResult(
            value=corrected_latency,
            is_valid=len(issues) == 0,
            issues=issues,
            original_value=float(raw_latency),
            confidence_score=confidence
        )

    def calculate_network_latency(self, device_id: str, sender_timestamp: Optional[int],
                                receive_timestamp: int) -> LatencyValidationResult:
        """Calculate network latency with clock drift correction."""
        issues = []

        if sender_timestamp is None:
            return LatencyValidationResult(None, False, [])

        # Validate timestamps
        if not TimestampValidator.validate_timestamp(sender_timestamp):
            issues.append(LatencyIssue.TIMESTAMP_ORDER_ERROR)
        if not TimestampValidator.validate_timestamp(receive_timestamp):
            issues.append(LatencyIssue.TIMESTAMP_ORDER_ERROR)

        if issues:
            return LatencyValidationResult(None, False, issues)

        raw_latency = receive_timestamp - sender_timestamp

        # Clock drift correction using per-device baseline
        if device_id not in self.baseline_corrections:
            self.baseline_corrections[device_id] = raw_latency

        # Update baseline if current measurement is more reasonable
        current_baseline = self.baseline_corrections[device_id]
        if abs(raw_latency) < abs(current_baseline):
            self.baseline_corrections[device_id] = raw_latency

        corrected_latency = raw_latency - self.baseline_corrections[device_id]

        # Detect clock drift
        if abs(raw_latency - current_baseline) > self.clock_drift_tolerance:
            issues.append(LatencyIssue.CLOCK_DRIFT)

        # Apply safeguards
        if corrected_latency < 0:
            issues.append(LatencyIssue.NEGATIVE_LATENCY)
            final_latency = 0.0
        elif corrected_latency > TimestampValidator.MAX_NETWORK_LATENCY:
            issues.append(LatencyIssue.EXCESSIVE_LATENCY)
            final_latency = float(TimestampValidator.MAX_NETWORK_LATENCY)
        else:
            final_latency = float(corrected_latency)

        # Calculate confidence score
        confidence = 1.0
        if issues:
            confidence = max(0.1, 1.0 - len(issues) * 0.25)
        if LatencyIssue.CLOCK_DRIFT in issues:
            confidence *= 0.7  # Lower confidence for clock drift cases

        return LatencyValidationResult(
            value=final_latency,
            is_valid=len([i for i in issues if i != LatencyIssue.CLOCK_DRIFT]) == 0,
            issues=issues,
            original_value=float(raw_latency),
            confidence_score=confidence
        )

    def calculate_local_processing_latency(self, start_timestamp: int, end_timestamp: int) -> LatencyValidationResult:
        """Calculate local processing latency."""
        issues = []

        raw_latency = end_timestamp - start_timestamp

        if raw_latency < 0:
            issues.append(LatencyIssue.NEGATIVE_LATENCY)
            corrected_latency = 0.0
        elif raw_latency > TimestampValidator.MAX_LOCAL_PROCESSING_LATENCY:
            issues.append(LatencyIssue.EXCESSIVE_LATENCY)
            corrected_latency = float(TimestampValidator.MAX_LOCAL_PROCESSING_LATENCY)
        else:
            corrected_latency = float(raw_latency)

        confidence = 1.0 if not issues else max(0.1, 1.0 - len(issues) * 0.4)

        return LatencyValidationResult(
            value=corrected_latency,
            is_valid=len(issues) == 0,
            issues=issues,
            original_value=float(raw_latency),
            confidence_score=confidence
        )

    def aggregate_latencies(self, latency_results: List[LatencyValidationResult]) -> Dict:
        """Aggregate latency results with outlier filtering."""
        valid_results = [r for r in latency_results if r.value is not None]

        if not valid_results:
            return {
                "min": None,
                "max": None,
                "avg": None,
                "count": 0,
                "confidence": 0.0,
                "issues_summary": {}
            }

        values = [r.value for r in valid_results]
        original_count = len(values)

        # Apply outlier filtering if enabled
        if self.enable_outlier_filtering and len(values) >= 4:
            filtered_values, removed_indices = OutlierFilter.filter_outliers(values, self.outlier_method)

            # Mark outliers in results
            for idx in removed_indices:
                valid_results[idx].issues.append(LatencyIssue.OUTLIER_DETECTED)
                valid_results[idx].confidence_score *= 0.3

            if filtered_values:
                values = filtered_values

        # Calculate statistics
        min_val = min(values)
        max_val = max(values)
        avg_val = statistics.mean(values)

        # Calculate overall confidence
        confidences = [r.confidence_score for r in valid_results]
        avg_confidence = statistics.mean(confidences) if confidences else 0.0

        # Summarize issues
        issues_summary = {}
        for result in valid_results:
            for issue in result.issues:
                issues_summary[issue.value] = issues_summary.get(issue.value, 0) + 1

        return {
            "min": min_val,
            "max": max_val,
            "avg": avg_val,
            "count": len(values),
            "original_count": original_count,
            "confidence": avg_confidence,
            "issues_summary": issues_summary
        }


# Factory function for easy integration with existing code
def create_robust_calculator(config: Optional[Dict] = None) -> RobustLatencyCalculator:
    """Create a RobustLatencyCalculator with sensible defaults."""
    default_config = {
        "clock_drift_tolerance_ms": 5000,
        "enable_outlier_filtering": True,
        "outlier_method": "iqr"
    }

    if config:
        default_config.update(config)

    return RobustLatencyCalculator(default_config)