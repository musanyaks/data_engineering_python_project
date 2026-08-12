"""Data validation utilities."""


import pandas as pd
from pydantic import BaseModel, ValidationError

from data_engineering.logger import get_logger

logger = get_logger("utils.validators")


def validate_schema(
    df: pd.DataFrame,
    schema: type[BaseModel],
    raise_on_error: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate DataFrame rows against a Pydantic schema.

    Args:
        df: DataFrame to validate.
        schema: Pydantic model class defining the expected schema.
        raise_on_error: If True, raise on first validation error.

    Returns:
        Tuple of (valid_rows, invalid_rows).

    Raises:
        ValidationError: If raise_on_error is True and invalid rows found.
    """
    valid_rows = []
    invalid_rows = []

    for idx, row in df.to_dict("records"):
        try:
            schema(**row)
            valid_rows.append(idx)
        except ValidationError as e:
            invalid_rows.append((idx, e))
            logger.warning(
                f"Validation failed for row {idx}",
                errors=e.errors(),
            )
            if raise_on_error:
                raise

    valid_df = df.loc[valid_rows].reset_index(drop=True)
    invalid_df = df.loc[[i for i, _ in invalid_rows]].reset_index(drop=True)

    logger.info(
        "Schema validation complete",
        valid=len(valid_df),
        invalid=len(invalid_df),
    )

    return valid_df, invalid_df


def check_nulls(df: pd.DataFrame, threshold: float = 0.1) -> dict[str, float]:
    """Check for columns with high null ratios.

    Args:
        df: DataFrame to check.
        threshold: Maximum acceptable null ratio (0-1).

    Returns:
        Dictionary of column names to null ratios that exceed threshold.
    """
    null_ratios = df.isnull().mean()
    high_nulls = null_ratios[null_ratios > threshold].to_dict()

    if high_nulls:
        logger.warning(
            "Columns with high null ratios detected",
            columns=high_nulls,
        )

    return high_nulls


def infer_types(df: pd.DataFrame) -> dict[str, str]:
    """Infer and return optimal data types for DataFrame columns.

    Args:
        df: DataFrame to analyze.

    Returns:
        Dictionary mapping column names to suggested types.
    """
    type_map = {}

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            type_map[col] = "datetime"
        elif pd.api.types.is_integer_dtype(df[col]):
            type_map[col] = "integer"
        elif pd.api.types.is_float_dtype(df[col]):
            type_map[col] = "float"
        elif pd.api.types.is_bool_dtype(df[col]):
            type_map[col] = "boolean"
        else:
            type_map[col] = "string"

    return type_map
