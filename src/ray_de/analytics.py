"""Typed analytical SELECT compiler; never accepts SQL or connection overrides."""
from typing import Literal
import math
import re

from pydantic import Field, model_validator
from .config import StrictModel
from .memory import SECRET_KEY

IDENTIFIER = r"^[A-Za-z_][A-Za-z0-9_]{0,127}$"


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(IDENTIFIER, value) or re.fullmatch(SECRET_KEY, value, re.I):
        raise ValueError("Unsupported analytical identifier")
    return "[" + value + "]"


class Metric(StrictModel):
    function: Literal["sum", "avg", "min", "max", "count", "count_distinct"]
    column: str


class Filter(StrictModel):
    column: str
    operator: Literal["eq", "ne", "lt", "le", "gt", "ge", "is_null", "is_not_null"]
    value: str | int | float | None = None


class Analytics(StrictModel):
    columns: list[str] = Field(default_factory=list, max_length=8)
    group_by: list[str] = Field(default_factory=list, max_length=3)
    metrics: list[Metric] = Field(default_factory=list, max_length=5)
    filters: list[Filter] = Field(default_factory=list, max_length=5)
    compare_schema: str = "dbo"
    compare_table: str = ""

    @model_validator(mode="after")
    def validate_inputs(self):
        for name in self.columns + self.group_by + [m.column for m in self.metrics] + [f.column for f in self.filters]:
            identifier(name)
        identifier(self.compare_schema)
        if self.compare_table:
            identifier(self.compare_table)
        for group in (self.columns, self.group_by):
            if len(group) != len(set(group)):
                raise ValueError("Analytical columns must be unique")
        for f in self.filters:
            if f.operator not in {"is_null", "is_not_null"} and f.value is None:
                raise ValueError("Use an explicit null operator")
            if f.operator in {"is_null", "is_not_null"} and f.value is not None:
                raise ValueError("Null operators do not take values")
            if isinstance(f.value, str) and len(f.value) > 500 or isinstance(f.value, float) and not math.isfinite(f.value):
                raise ValueError("Filter value is not bounded")
        return self


def validate_operation(operation, spec):
    if operation == "lakehouse_profile":
        if not spec.columns or spec.group_by or spec.metrics or spec.compare_table:
            raise ValueError("Profile requires columns and optional filters only")
    elif operation == "lakehouse_aggregate":
        if not spec.metrics or spec.columns or spec.compare_table:
            raise ValueError("Aggregation requires metrics, optional grouping and filters")
    elif operation == "lakehouse_compare":
        if not spec.columns or not spec.compare_table or spec.metrics or spec.group_by or spec.filters:
            raise ValueError("Comparison requires columns and a same-lakehouse target table only")
    else:
        raise ValueError("Unsupported analytical operation")


def compile_select(operation, schema, table_name, arguments):
    spec = Analytics.model_validate(arguments)
    validate_operation(operation, spec)
    table = identifier(schema) + "." + identifier(table_name)
    conditions, params = [], []
    operators = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
    for f in spec.filters:
        column = identifier(f.column)
        if f.operator in {"is_null", "is_not_null"}:
            conditions.append(column + (" IS NULL" if f.operator == "is_null" else " IS NOT NULL"))
        else:
            conditions.append(column + " " + operators[f.operator] + " ?")
            params.append(f.value)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    if operation == "lakehouse_profile":
        expressions = ["COUNT_BIG(*) AS row_count"]
        for index, name in enumerate(spec.columns):
            column = identifier(name)
            expressions += [f"SUM(CASE WHEN {column} IS NULL THEN CAST(1 AS bigint) ELSE 0 END) AS c{index}_nulls",
                            f"COUNT_BIG(DISTINCT {column}) AS c{index}_distinct"]
        return "SELECT " + ", ".join(expressions) + " FROM " + table + where, tuple(params)
    if operation == "lakehouse_aggregate":
        groups = [identifier(c) for c in spec.group_by]
        expressions = list(groups)
        for index, metric in enumerate(spec.metrics):
            column = identifier(metric.column)
            expression = (f"COUNT_BIG(DISTINCT {column})" if metric.function == "count_distinct"
                          else f"{'COUNT_BIG' if metric.function == 'count' else metric.function.upper()}({column})")
            expressions.append(expression + f" AS metric_{index}")
        group = " GROUP BY " + ", ".join(groups) if groups else ""
        order = " ORDER BY " + ", ".join(groups) if groups else ""
        return "SELECT TOP (101) " + ", ".join(expressions) + " FROM " + table + where + group + order, tuple(params)
    target = identifier(spec.compare_schema) + "." + identifier(spec.compare_table)
    columns = ", ".join(identifier(c) for c in spec.columns)
    count_name = "__ray_multiplicity"
    while count_name.lower() in {c.lower() for c in spec.columns}:
        count_name += "_"
    count_column = identifier(count_name)
    join = " AND ".join(f"(s.{identifier(c)} = t.{identifier(c)} OR (s.{identifier(c)} IS NULL AND t.{identifier(c)} IS NULL))" for c in spec.columns)
    # Compare row multiplicities, not a fan-out join. Nulls compare equal. Strings
    # use the database collation; the returned coverage documents that boundary.
    sql = (f"WITH s AS (SELECT {columns}, COUNT_BIG(*) AS {count_column} FROM {table} GROUP BY {columns}), "
           f"t AS (SELECT {columns}, COUNT_BIG(*) AS {count_column} FROM {target} GROUP BY {columns}) "
           f"SELECT COALESCE(SUM(CASE WHEN COALESCE(s.{count_column},0) > COALESCE(t.{count_column},0) "
           f"THEN COALESCE(s.{count_column},0)-COALESCE(t.{count_column},0) ELSE 0 END),0) AS missing_from_target, "
           f"COALESCE(SUM(CASE WHEN COALESCE(t.{count_column},0) > COALESCE(s.{count_column},0) "
           f"THEN COALESCE(t.{count_column},0)-COALESCE(s.{count_column},0) ELSE 0 END),0) AS extra_in_target "
           f"FROM s FULL OUTER JOIN t ON {join}")
    return sql, ()
