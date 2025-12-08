'''
ResultComparator compares the agent's actual SQL result
against the expected result and returns a ComparisonResult.
'''


from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
from pandas import DataFrame

from data_structures import ComparisonResult  


class ResultComparator(ABC):
    """
    Base interface for all result comparison strategies.

    Responsibilities:
        - Compare actual results vs expected results.
        - Return a ComparisonResult with detailed match/mismatch info.
    """

    @abstractmethod
    def compare(self, actual: Any, expected: Any) -> ComparisonResult:
        raise NotImplementedError


class ExactMatchComparator(ResultComparator):
    """
    Concrete comparator that checks for (almost) exact equality between
    the actual and expected results.

    Assumptions:
        - 'actual' and 'expected' are:
            - either pandas DataFrames, OR
            - data that can be converted to DataFrames.
        - Column names matter.
        - Row contents must match exactly.
        - Row order is IGNORED (we sort by all columns before comparing).

    Behavior:
        - If shapes, column sets, and all values match -> is_correct = True.
        - Otherwise:
            - compute row_match_ratio and column_match_ratio,
            - fill missing_columns and extra_columns,
            - provide a human-readable message.
    """

    def _to_dataframe(self, value: Any, label: str) -> DataFrame:
        """
        Helper to convert arbitrary value into a pandas DataFrame.
        """
        if isinstance(value, pd.DataFrame):
            return value.copy()

        try:
            df = pd.DataFrame(value)
            return df
        except Exception as exc:
            raise TypeError(
                f"Cannot convert {label} to pandas DataFrame for comparison: {exc}"
            ) from exc

    def compare(self, actual: Any, expected: Any) -> ComparisonResult:
        # Convert both to DataFrames
        actual_df = self._to_dataframe(actual, "actual")
        expected_df = self._to_dataframe(expected, "expected")

        # Normalize column order: line up expected columns first
        actual_cols = list(actual_df.columns)
        expected_cols = list(expected_df.columns)

        # Compute column-level info
        expected_set = set(expected_cols)
        actual_set = set(actual_cols)

        missing_columns = sorted(list(expected_set - actual_set))
        extra_columns = sorted(list(actual_set - expected_set))
        common_columns = sorted(list(expected_set & actual_set))

        # Column match ratio: fraction of expected columns present
        column_match_ratio = (
            len(common_columns) / len(expected_cols) if expected_cols else 1.0
        )

        # If there are no common columns - incorrect
        if not common_columns:
            return ComparisonResult(
                is_correct=False,
                row_match_ratio=0.0,
                column_match_ratio=column_match_ratio,
                numeric_tolerance_ok=False,
                missing_columns=missing_columns,
                extra_columns=extra_columns,
                message="No overlapping columns between actual and expected results.",
            )

        # Restrict both DataFrames to the common columns in the same order
        actual_common = actual_df[common_columns].copy()
        expected_common = expected_df[common_columns].copy()

        # Sort rows by all common columns so row order doesn't affect equality
        actual_sorted = actual_common.sort_values(by=common_columns).reset_index(drop=True)
        expected_sorted = expected_common.sort_values(by=common_columns).reset_index(drop=True)

        # Exact equality check
        frames_equal = actual_sorted.equals(expected_sorted)

        # Compute row_match_ratio as similarity of row sets
        #treat rows as tuples across all common columns.
        actual_rows = [tuple(row) for _, row in actual_sorted.iterrows()]
        expected_rows = [tuple(row) for _, row in expected_sorted.iterrows()]

        actual_row_set = set(actual_rows)
        expected_row_set = set(expected_rows)

        if expected_rows:
            matching_rows = len(actual_row_set & expected_row_set)
            total_expected_rows = len(expected_row_set)
            row_match_ratio = matching_rows / total_expected_rows
        else:
            # If expected is empty:
            row_match_ratio = 1.0 if not actual_rows else 0.0

        # For this exact comparator, numeric_tolerance_ok is just equality
        numeric_tolerance_ok = frames_equal

        if frames_equal and not missing_columns and not extra_columns:
            message = "Actual result matches expected result exactly."
        else:
            message = (
                f"Exact match failed. "
                f"Row match ratio: {row_match_ratio:.2f}, "
                f"Column match ratio: {column_match_ratio:.2f}. "
            )
            if missing_columns:
                message += f"Missing columns: {missing_columns}. "
            if extra_columns:
                message += f"Extra columns: {extra_columns}. "

        return ComparisonResult(
            is_correct=frames_equal and not missing_columns and not extra_columns,
            row_match_ratio=row_match_ratio,
            column_match_ratio=column_match_ratio,
            numeric_tolerance_ok=numeric_tolerance_ok,
            missing_columns=missing_columns,
            extra_columns=extra_columns,
            message=message,
        )