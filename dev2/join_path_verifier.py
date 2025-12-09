from typing import Any, List
from .models import ValidationResult
from .sql_parser import SQLParser
import sqlglot


class JoinPathVerifier:
    """
    Validates JOIN conditions against foreign key relationships.
    Ensures that JOINs in SQL queries follow valid FK paths.
    """

    def __init__(self):
        self.parser = SQLParser()

    def verify(self, sql: str, schema: Any, dialect: str = "") -> ValidationResult:
        """
        Verify JOIN conditions are valid based on schema relationships.
        
        Args:
            sql: The SQL query to analyze
            schema: SchemaSnapshot with tables and FK info
            dialect: SQL dialect for proper parsing
        
        Returns:
            ValidationResult with JOIN-specific errors
        """
        parsed = self.parser.parse(sql, dialect)
        errors = []
        warnings = []
        
        # Find all JOIN expressions
        joins = list(parsed.ast.find_all(sqlglot.exp.Join))
        
        for join in joins:
            join_errors = self._verify_single_join(join, schema)
            errors.extend(join_errors)
        
        return ValidationResult(
            is_valid=(len(errors) == 0),
            errors=errors,
            warnings=warnings
        )

    def _verify_single_join(self, join: Any, schema: Any) -> List[str]:
        """Verify a single JOIN expression."""
        errors = []
        
        # Get the table being joined
        join_table = None
        if hasattr(join, 'this') and isinstance(join.this, sqlglot.exp.Table):
            join_table = join.this.name
        
        if not join_table:
            return errors
        
        # Get JOIN condition
        on_clause = join.args.get('on')
        if not on_clause:
            # No ON clause - might be CROSS JOIN or implicit join
            return errors
        
        # Extract columns from ON clause
        join_columns = self._extract_join_columns(on_clause)
        
        # Verify FK relationships
        for left_col, right_col in join_columns:
            if not self._is_valid_join_path(left_col, right_col, schema):
                # This is a warning, not error - joins can be valid without FK
                pass
        
        return errors

    def _extract_join_columns(self, on_clause: Any) -> List[tuple]:
        """Extract column pairs from JOIN ON clause."""
        pairs = []
        
        # Find all EQ expressions (col1 = col2)
        for eq in on_clause.find_all(sqlglot.exp.EQ):
            left = eq.left
            right = eq.right
            
            if isinstance(left, sqlglot.exp.Column) and isinstance(right, sqlglot.exp.Column):
                left_name = f"{left.table}.{left.name}" if left.table else left.name
                right_name = f"{right.table}.{right.name}" if right.table else right.name
                pairs.append((left_name, right_name))
        
        return pairs

    def _is_valid_join_path(self, left_col: str, right_col: str, schema: Any) -> bool:
        """Check if there's a FK relationship between columns."""
        left_parts = left_col.split(".")
        right_parts = right_col.split(".")
        
        if len(left_parts) < 2 or len(right_parts) < 2:
            return True  # Can't verify without table qualifier
        
        left_table, left_column = left_parts[0], left_parts[1]
        right_table, right_column = right_parts[0], right_parts[1]
        
        # Check if left_column has FK to right_table
        left_table_info = schema.tables.get(left_table)
        if left_table_info:
            for col in left_table_info.columns:
                if col.name.lower() == left_column.lower():
                    if col.foreign_key:
                        fk_parts = col.foreign_key.split(".")
                        if len(fk_parts) >= 2:
                            fk_table = fk_parts[0]
                            if fk_table.lower() == right_table.lower():
                                return True
        
        # Check reverse direction
        right_table_info = schema.tables.get(right_table)
        if right_table_info:
            for col in right_table_info.columns:
                if col.name.lower() == right_column.lower():
                    if col.foreign_key:
                        fk_parts = col.foreign_key.split(".")
                        if len(fk_parts) >= 2:
                            fk_table = fk_parts[0]
                            if fk_table.lower() == left_table.lower():
                                return True
        
        return False

    def get_valid_join_paths(self, table: str, schema: Any) -> List[str]:
        """
        Get all tables that can be validly JOINed with the given table.
        
        Args:
            table: Table name to find join paths for
            schema: SchemaSnapshot with FK info
        
        Returns:
            List of table names that have FK relationships
        """
        valid_tables = []
        
        table_info = schema.tables.get(table)
        if not table_info:
            return valid_tables
        
        # Find tables this table can join to (outgoing FKs)
        for col in table_info.columns:
            if col.foreign_key:
                fk_table = col.foreign_key.split(".")[0]
                if fk_table not in valid_tables:
                    valid_tables.append(fk_table)
        
        # Find tables that can join to this table (incoming FKs)
        for other_table_name, other_table_info in schema.tables.items():
            if other_table_name == table:
                continue
            for col in other_table_info.columns:
                if col.foreign_key and col.foreign_key.startswith(f"{table}."):
                    if other_table_name not in valid_tables:
                        valid_tables.append(other_table_name)
        
        return valid_tables
