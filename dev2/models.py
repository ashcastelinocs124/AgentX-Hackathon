from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass
class IdentifierSet:
    """Extracted SQL identifiers."""
    tables: List[str]
    columns: List[str]
    functions: List[str]
    aliases: Dict[str, str]


@dataclass
class HallucinationReport:
    """Report of phantom identifiers in SQL."""
    phantom_tables: List[str]
    phantom_columns: List[str]
    phantom_functions: List[str]
    total_hallucinations: int = 0
    hallucination_score: float = 0.0

    def __post_init__(self):
        self.total_hallucinations = (
            len(self.phantom_tables) +
            len(self.phantom_columns) +
            len(self.phantom_functions)
        )


@dataclass
class ValidationResult:
    """Result of SQL validation against schema."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    hallucination_report: Optional[HallucinationReport] = None
