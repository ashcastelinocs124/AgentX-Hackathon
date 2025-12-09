from .models import IdentifierSet, ValidationResult
from typing import Any, List


class SchemaValidator:
    """
    Validates SQL identifiers against a database schema.
    Checks if tables and columns actually exist.
    """

    def validate(self, identifiers: IdentifierSet, schema: Any) -> ValidationResult:
        """
        Validate extracted identifiers against schema.
        
        Args:
            identifiers: IdentifierSet from SQLParser
            schema: SchemaSnapshot from Dev 1's infrastructure
        
        Returns:
            ValidationResult with errors and warnings
        """
        errors = []
        warnings = []
        
        # Check tables exist
        for table in identifiers.tables:
            table_name = table.split(".")[-1]  # Handle qualified names
            if not schema.has_table(table_name) and not schema.has_table(table):
                errors.append(f"Table '{table}' does not exist.")
        
        # Check columns exist
        for col in identifiers.columns:
            if not self._column_exists(col, identifiers.tables, identifiers.aliases, schema):
                errors.append(f"Column '{col}' does not exist in any table.")
        
        return ValidationResult(
            is_valid=(len(errors) == 0),
            errors=errors,
            warnings=warnings
        )

    def _column_exists(
        self, 
        col: str, 
        tables: List[str], 
        aliases: dict, 
        schema: Any
    ) -> bool:
        """Check if a column exists in any of the referenced tables."""
        col_parts = col.split(".")
        col_name = col_parts[-1].lower()
        table_qualifier = col_parts[0].lower() if len(col_parts) > 1 else None
        
        # If column has table qualifier, check that specific table
        if table_qualifier:
            # Check if qualifier is an alias
            actual_table = aliases.get(table_qualifier, table_qualifier)
            table_info = schema.tables.get(actual_table)
            if table_info:
                return col_name in [c.name.lower() for c in table_info.columns]
            return False
        
        # Otherwise, check all referenced tables
        for table in tables:
            table_name = table.split(".")[-1]
            table_info = schema.tables.get(table_name) or schema.tables.get(table)
            if table_info:
                if col_name in [c.name.lower() for c in table_info.columns]:
                    return True
        
        # Also check all tables in schema (for unqualified columns)
        for table_info in schema.tables.values():
            if col_name in [c.name.lower() for c in table_info.columns]:
                return True
        
        return False
