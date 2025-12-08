'''
Scorer combines:
- correctness (from ComparisonResult)
- efficiency (from timing + QueryPlan)
- safety (from hallucination info)
into a MultiDimensionalScore for a single task.
'''

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

from data_structures import (
    ComparisonResult,
    QueryPlan,
    MultiDimensionalScore,
)


class Scorer(ABC):
    """
    Interface for computing multi-dimensional scores for a task.

    Responsibilities:
        - Combine correctness, efficiency, and safety into a MultiDimensionalScore.
    """

    @abstractmethod
    def score(
        self,
        comparison: ComparisonResult,
        timing_ms: float,
        query_plan: Optional[QueryPlan] = None,
        hallucination_info: Optional[Dict[str, Any]] = None,
    ) -> MultiDimensionalScore:
        """
        Compute and return a MultiDimensionalScore for one task.
        """
        raise NotImplementedError


class DefaultScorer(Scorer):
    """
    Default implementation of Scorer.

    Scoring:
        - correctness:
            - primarily from comparison.is_correct,
            - but also uses row_match_ratio and column_match_ratio
              to give partial credit.
        - efficiency:
            - based on execution time (ms),
            - optionally uses QueryPlan.estimated_cost if available.
        - safety:
            - penalizes hallucinations based on hallucination_info signals.

    All three are combined into an overall score via weighted average.
    """

    def __init__(
        self,
        weight_correctness: float = 0.6,
        weight_efficiency: float = 0.2,
        weight_safety: float = 0.2,
    ) -> None:
        """
        Initialize the scorer with weights for each dimension.

        Parameters:
            weight_correctness -> Weight for correctness score.
            weight_efficiency  -> Weight for efficiency score.
            weight_safety      -> Weight for safety score.
        """
        self.weight_correctness = weight_correctness
        self.weight_efficiency = weight_efficiency
        self.weight_safety = weight_safety


    def _score_correctness(self, comparison: ComparisonResult) -> float:
        """
        Compute a correctness score in [0, 1].

        Strategy:
            - If is_correct is True, return 1.0.
            - Otherwise, blend row_match_ratio and column_match_ratio
              to give partial credit.
        """
        if comparison.is_correct:
            return 1.0

        # 70% rows, 30% columns
        score = 0.7 * comparison.row_match_ratio + 0.3 * comparison.column_match_ratio
        # Clamp to [0, 1]
        return max(0.0, min(1.0, score))

    def _score_efficiency(
        self,
        timing_ms: float,
        query_plan: Optional[QueryPlan] = None,
    ) -> float:
        """
        Compute an efficiency score in [0, 1].

        Strategy (simple, hackathon-friendly):
            - Use a decaying function of execution time:
                - very fast queries (~0-100 ms) -> near 1.0
                - slower queries -> gradually lower score.
            - Optionally adjust based on estimated_cost if available.
        """

        # Base on timing: 1 / (1 + t/1000) maps:
        #   0 ms   -> 1.0
        #   100 ms -> ~0.91
        #   500 ms -> ~0.67
        #   1000ms -> 0.5
        base_eff = 1.0 / (1.0 + (timing_ms / 1000.0))

        # Adjust slightly if estimated_cost is available
        if query_plan is not None and query_plan.estimated_cost is not None:
            cost = query_plan.estimated_cost
            # Simple normalization: assume "reasonable" cost up to 100000.
            # Larger costs get penalized.
            cost_factor = 1.0 / (1.0 + (cost / 100000.0))
            eff = base_eff * cost_factor
        else:
            eff = base_eff

        # Clamp to [0, 1]
        return max(0.0, min(1.0, eff))

    def _score_safety(self, hallucination_info: Optional[Dict[str, Any]]) -> float:
        """
        Compute a safety score in [0, 1].

        Strategy:
            - If no hallucination_info is provided, assume safe (score = 1.0).
            - If hallucination_info contains:
                - 'has_hallucination': bool
                  - True  -> strong penalty
                  - False -> full score
                - OR 'score': float in [0, 1] (already a safety-like score).
        """
        if hallucination_info is None:
            return 1.0

        # Case 1: direct score provided
        if "score" in hallucination_info:
            raw_score = float(hallucination_info["score"])
            return max(0.0, min(1.0, raw_score))

        # Case 2: has_hallucination flag
        if hallucination_info.get("has_hallucination") is True:
            # Hard penalty if hallucination detected
            return 0.0

        # If we explicitly know there is no hallucination
        if hallucination_info.get("has_hallucination") is False:
            return 1.0

        # Default: unknown -> high score
        return 0.9

    # ---------- Main scoring entrypoint ----------

    def score(
        self,
        comparison: ComparisonResult,
        timing_ms: float,
        query_plan: Optional[QueryPlan] = None,
        hallucination_info: Optional[Dict[str, Any]] = None,
    ) -> MultiDimensionalScore:
        """
        Compute and return a MultiDimensionalScore for one task.
        """

        correctness = self._score_correctness(comparison)
        efficiency = self._score_efficiency(timing_ms, query_plan)
        safety = self._score_safety(hallucination_info)

        # Weighted overall score
        total_weight = (
            self.weight_correctness + self.weight_efficiency + self.weight_safety
        )
        overall = (
            correctness * self.weight_correctness
            + efficiency * self.weight_efficiency
            + safety * self.weight_safety
        ) / total_weight

        # Clamp overall just in case
        overall = max(0.0, min(1.0, overall))

        details = {
            "correctness_component": correctness,
            "efficiency_component": efficiency,
            "safety_component": safety,
            "weights": {
                "correctness": self.weight_correctness,
                "efficiency": self.weight_efficiency,
                "safety": self.weight_safety,
            },
            "timing_ms": timing_ms,
        }

        if query_plan is not None:
            details["query_plan_cost"] = query_plan.estimated_cost
            details["query_plan_rows"] = query_plan.estimated_rows

        if hallucination_info is not None:
            details["hallucination_info"] = hallucination_info

        return MultiDimensionalScore(
            correctness=correctness,
            efficiency=efficiency,
            safety=safety,
            overall=overall,
            details=details,
        )