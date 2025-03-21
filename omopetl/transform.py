import hashlib
import os
import uuid
import warnings

import numpy as np
import pandas as pd


class Transformer:
    def __init__(self, data, project_path, source_schema, target_schema, table_name):
        """
        Initialize the Transformer with source data, project path, and schema paths.

        Parameters:
        - data: DataFrame containing the source data.
        - project_path: Path to the project directory.
        - source_schema_path: Path to the source schema YAML file.
        - target_schema_path: Path to the target schema YAML file.
        - table_name: Name of the target table.
        """
        self.data = data
        self.project_path = project_path
        self.source_schema = source_schema
        self.target_schema = target_schema
        self.table_name = table_name
        self.lookup_cache = {}

    def _get_column_type(self, table_name, column_name, schema, strict):
        """
        Retrieve the data type for a column from the schema.

        Parameters:
        - table_name: Name of the table in the schema.
        - column_name: Name of the column whose type is to be retrieved.
        - schema: Schema dictionary to search.
        - strict: If True, raise errors on schema issues.

        Returns:
        - str: The data type of the column.

        Raises:
        - KeyError: If the table or column is not found in the schema.
        """
        table = schema.get(table_name, {})
        columns = table.get("columns", {})
        column = columns.get(column_name)

        if not column:
            if strict:
                raise KeyError(f"Data type for '{column_name}' not found in '{table_name}' schema.")
            else:
                return None

        return column["type"]

    def _load_lookup_table(self, lookup_name, file_extension="csv"):
        """
        Load a lookup table from the lookups directory.

        Parameters:
        - lookup_name: The name of the lookup file (without extension).
        - file_extension: File extension (default is "csv").

        Returns:
        - pd.DataFrame: The loaded lookup table.
        """
        if lookup_name in self.lookup_cache:
            return self.lookup_cache[lookup_name]

        lookup_path = os.path.join(self.project_path, "data", "lookups", f"{lookup_name}.{file_extension}")
        if not os.path.exists(lookup_path):
            raise FileNotFoundError(f"Lookup table '{lookup_name}' not found at '{lookup_path}'.")

        if file_extension == "csv":
            lookup_table = pd.read_csv(lookup_path)
        elif file_extension == "parquet":
            lookup_table = pd.read_parquet(lookup_path)
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")

        self.lookup_cache[lookup_name] = lookup_table
        return lookup_table

    def apply_transformations(self, sequence, strict=True):
        """
        Apply transformations based on column mappings.

        Parameters:
        - sequence: List of mappings with transformation details.

        Returns:
        - DataFrame: Transformed data with only the specified columns.
        """
        transformed_data = pd.DataFrame()

        for step in sequence:
            target_column = step["add_column"]
            transformations = step.get("transformation")

            # Normalize transformation to always be a list
            if isinstance(transformations, dict):
                transformations = [transformations]  # Convert single dict to list
            elif isinstance(transformations, list):
                pass
            else:
                raise ValueError(f"Invalid transformations format for column '{target_column}'")

            if not transformations or not isinstance(transformations, list):
                raise ValueError(f"Invalid or missing transformations for column '{target_column}'")

            # Fetch source columns from the transformation section
            source_columns = transformations[0].get("source_columns") or [transformations[0].get("source_column")]

            # Handle the initial data for the transformation
            # For a link transformation, initial data is not from self.data
            if transformations[0]["type"] == "link":
                current_data = None
            else:
                current_data = self.data[source_columns] if source_columns[0] else None

            # Chain the transformations
            for transformation in transformations:
                transform_type = transformation["type"]

                # Load the transformation method
                method = getattr(self, f"transform_{transform_type}", None)
                if method is None:
                    raise ValueError(f"Unsupported transformation type: {transform_type}")

                # Apply the transformation
                current_data = method(current_data, target_column, transformation)

            # Get target data type from the target schema
            target_type = self._get_column_type(self.table_name, target_column, self.target_schema, strict)

            # Cast the transformed data to the target type
            if strict:
                current_data = self.cast_to_type(current_data, target_type)

            # Store the final transformed column
            transformed_data[target_column] = current_data

        # Reorder columns based on target schema
        expected_order = list(self.target_schema.get(self.table_name, {}).get("columns", {}).keys())
        transformed_data = transformed_data.reindex(columns=expected_order)

        # Validate relationships
        self._validate_relationships(transformed_data)

        return transformed_data

    def cast_to_type(self, series, data_type):
        """Cast a pandas Series to the specified data type."""
        try:
            normalized_type = data_type.lower()
            if normalized_type == "string":
                return series.astype(str)
            elif normalized_type == "integer":
                return series.astype("Int64")
            elif normalized_type == "float":
                return series.astype(float)
            elif normalized_type == "boolean":
                return series.astype(bool)
            elif normalized_type == "date":
                return pd.to_datetime(series, errors="coerce").dt.date
            elif normalized_type == "datetime":
                return pd.to_datetime(series, errors="coerce")
            else:
                raise ValueError(f"Unsupported data type: {normalized_type}")
        except Exception as e:
            raise ValueError(f"Error casting to {normalized_type}: {e}")

    def _validate_relationships(self, transformed_data):
        """
        Validate source and target rows.
        """
        # Check alignment of rows against original data
        if len(self.data) != len(transformed_data):
            raise ValueError("Row count mismatch after transformations. Relationships may be broken.")

    # Transformation methods
    def transform_map(self, current_data, target_column, transformation):
        """Map values in the source column to new values."""
        source_column = transformation.get("source_column")
        value_map = transformation["values"]
        return current_data[source_column].map(value_map)

    def transform_copy(self, current_data, target_column, transformation):
        """Copy values from the source column."""
        source_column = transformation["source_column"]
        return current_data[source_column]

    def transform_link(self, current_data, target_column, transformation):
        """
        Handle linked table transformation with optional aggregation.

        Parameters:
        - source_column: The column in the linked table to use.
        - target_column: *Not used*. Overwritten by the column specified in the transform.
        - transformation: Dictionary containing transformation details.

        Returns:
        - Series: The transformed column data.
        """
        linked_target_table_name = transformation["linked_table"]
        if not linked_target_table_name:
            raise KeyError("'linked_table' is required for a 'link' transformation.")

        link_column = transformation["link_column"]
        if not link_column:
            raise KeyError("'link_column' is required for a 'link' transformation.")

        source_column = transformation["source_column"]

        # Optional ordering for aggregation
        order_by = transformation.get("order_by")

        # List of directories to check in priority order
        search_dirs = ["source", "lookups", "target"]

        # Iterate over locations to find the linked table
        linked_table_path = None
        for directory in search_dirs:
            path = os.path.join(self.project_path, "data", directory, f"{linked_target_table_name}.csv")
            if os.path.exists(path):
                linked_table_path = path
                break

        if not linked_table_path:
            msg = f"Linked table '{linked_target_table_name}' not found in 'source', 'lookups', or 'target' directories."
            raise FileNotFoundError(msg)

        if "delimiter" in transformation:
            linked_table = pd.read_csv(linked_table_path, sep=transformation["delimiter"])
        else:
            linked_table = pd.read_csv(linked_table_path)

        # Handle aggregation if specified
        aggregation = transformation.get("aggregation")
        if aggregation:
            method = aggregation.get("method", "first")

            # Apply ordering if specified
            if order_by:
                linked_table = linked_table.sort_values(order_by)

            if method == "most_frequent":
                aggregated_data = (
                    linked_table.groupby(link_column)[source_column]
                    .agg(lambda x: x.value_counts().idxmax())
                    .reset_index()
                )
            elif method == "first":
                aggregated_data = linked_table.groupby(link_column).first().reset_index()
            elif method == "last":
                aggregated_data = linked_table.groupby(link_column).last().reset_index()
            else:
                raise ValueError(f"Unknown aggregation method: {method}")
        else:
            aggregated_data = linked_table[[link_column, source_column]]

        # Merge aggregated data with the base table
        self.data = self.data.merge(
            aggregated_data,
            how="left",
            left_on=link_column,
            right_on=link_column,
            suffixes=("", "")
        )

        # Return the linked column data
        return self.data[source_column]

    def transform_aggregate(self, current_data, target_column, transformation):
        """
        Perform an aggregation on the specified source columns.

        Parameters:
        - current_data: DataFrame with the source data.
        - target_column: The column in the target table to store the aggregated result.
        - transformation: Dictionary containing transformation details.

        Returns:
        - Series: The aggregated column data.
        """
        source_columns = transformation.get("source_columns")
        if not source_columns:
            raise ValueError(f"'source_columns' must be specified for aggregation in '{target_column}'.")

        group_by = transformation.get("group_by")
        if not group_by:
            raise ValueError(f"'group_by' must be specified for aggregation in '{target_column}'.")

        # Ensure `group_by` is a list
        if isinstance(group_by, str):
            group_by = [group_by]

        order_by = transformation.get("order_by", None)
        aggregation = transformation.get("aggregation", "first")

        # Ensure all required columns exist
        missing_columns = [col for col in source_columns if col not in current_data.columns]
        if missing_columns:
            raise KeyError(f"Columns {missing_columns} not found in source data.")

        # Apply order_by only to the subset of data being aggregated
        if order_by:
            aggregated_data = current_data.sort_values(order_by).groupby(group_by)
        else:
            aggregated_data = current_data.groupby(group_by)

        # Perform aggregation
        if aggregation == "first":
            aggregated_data = aggregated_data.first().reset_index()
        elif aggregation == "last":
            aggregated_data = aggregated_data.last().reset_index()
        elif aggregation == "most_frequent":
            aggregated_data = (
                aggregated_data.agg(lambda x: x.value_counts().idxmax()).reset_index()
            )
        else:
            raise ValueError(f"Unsupported aggregation method: {aggregation}")

        # Merge aggregated results back into current_data
        merge_columns = list(set(group_by + [target_column]))

        current_data = current_data.merge(aggregated_data[merge_columns],
                                          on=group_by,
                                          how="left",
                                          suffixes=("", "_agg"))

        return current_data[f"{target_column}_agg"].rename(target_column)

    def transform_lookup(self, current_data, target_column, transformation):
        """
        Perform a lookup transformation using a lookup table.

        Parameters:
        - current_data: Current DataFrame or Series being transformed.
        - target_column: The target column name for the result.
        - transformation: Transformation details including lookup table and columns.

        Returns:
        - pd.Series: The result of the lookup transformation.
        """
        source_column = transformation["source_column"]
        lookup_name = transformation["vocabulary"]
        source_lookup_column = transformation.get("source_lookup_column")
        target_lookup_column = transformation.get("target_lookup_column")
        default_value = transformation.get("default_value", None)

        # Load the lookup table
        lookup_table = self._load_lookup_table(lookup_name)

        # Validate lookup table structure
        if source_lookup_column not in lookup_table.columns or target_lookup_column not in lookup_table.columns:
            raise KeyError(
                f"Lookup table '{lookup_name}' is missing required columns: "
                f"'{source_lookup_column}' or '{target_lookup_column}'."
            )

        # Perform the lookup
        lookup_dict = lookup_table.set_index(source_lookup_column)[target_lookup_column].to_dict()
        result = current_data[source_column].map(lookup_dict)

        # Handle unmapped values
        if default_value is not None:
            result = result.fillna(default_value)

        # Cast to the expected target type
        # target_type = self._get_column_type(self.table_name, target_column, self.target_schema, strict=True)
        # result = self.cast_to_type(result, target_type)

        return result

    def transform_normalize_date(self, current_data, target_column, transformation):
        """Normalize date values to a specific format."""
        source_column = transformation.get("source_column")
        date_format = transformation.get("format", "%Y-%m-%d")
        return pd.to_datetime(current_data[source_column], errors="coerce").dt.strftime(date_format)

    def transform_concatenate(self, current_data, target_column, transformation):
        """
        Concatenate multiple columns into a single column.

        Parameters:
        - source_columns: List of columns to concatenate.
        - target_column: The resulting target column name.
        - transformation: Dictionary containing transformation details.

        Returns:
        - Series: Concatenated column as a pandas Series.
        """
        source_columns = transformation.get("source_columns")
        if not source_columns:
            raise KeyError("source_columns is required for concatenate transformation.")
        separator = transformation.get("separator", "-")
        return current_data[source_columns].astype(str).agg(separator.join, axis=1)

    def transform_default(self, current_data, target_column, transformation):
        """
        Assign a default value.

        Parameters:
        - source_column: Ignored for default transformations.
        - target_column: The name of the target column to populate.
        - transformation: Dictionary containing the default value.

        Returns:
        - Series: A pandas Series filled with the default value, matching the length of the source data.
        """
        default_value = transformation["value"]
        return pd.Series(default_value, index=self.data.index)

    def transform_conditional_map(self, current_data, target_column, transformation):
        """
        Apply conditional mappings to the source column, with optional default value.

        Parameters:
        - current_data: Current data being processed.
        - target_column: The target column name (not used here).
        - transformation: Dictionary containing transformation details, including conditions.

        Returns:
        - Series: The column with conditionally mapped values.
        """
        conditions = transformation["conditions"]
        # Optional default value
        default_value = transformation.get("default", None)
        result_column = pd.Series(default_value, index=self.data.index, dtype="Int64")

        for condition in conditions:
            condition_str = condition["condition"]
            value = condition["value"]
            # Apply the condition and assign the value
            mask = current_data.eval(condition_str)
            result_column[mask] = value

        return result_column

    def transform_filter(self, current_data, target_column, transformation):
        """
        Filters rows in the dataset based on a query condition.

        Parameters:
        - current_data: DataFrame being processed.
        - target_column: Not used in filtering, included for consistency.
        - transformation: Dictionary containing transformation details.

        Returns:
        - DataFrame: The filtered data.
        """
        condition = transformation.get("condition")
        if not condition:
            raise ValueError("A 'condition' must be specified for the 'filter' transformation.")

        # Ensure current_data is a DataFrame before querying
        if isinstance(current_data, pd.Series):
            current_data = current_data.to_frame() 

        # Apply filter using Pandas `query()`
        return current_data.query(condition)

    def transform_derive(self, current_data, target_column, transformation):
        """Calculate derived values using a formula."""
        formula = transformation["formula"]

        if isinstance(current_data, pd.DataFrame):
            return current_data.eval(formula)
        elif isinstance(current_data, pd.Series):
            return pd.eval(formula, local_dict={"number": current_data})
        else:
            raise TypeError(f"Unsupported data type for eval: {type(current_data)}")

    def transform_generate_id(self, current_data, target_column, transformation):
        """
        Generate a unique ID for each row based on different methods.

        Supported methods:
        - 'uuid'         → Generates a UUID (default).
        - 'incremental'  → Generates sequential numbers starting from 1.
        - 'hash'         → Generates a SHA256 hash from a source column.

        Parameters:
        - current_data: DataFrame of the source data.
        - target_column: The column to store the generated IDs.
        - transformation: Dictionary containing transformation details.

        Returns:
        - Series: The generated ID column.
        """
        # Default to uuid
        method = transformation.get("method", "uuid")
        source_column = transformation.get("source_column", None)

        # Ensure we have a valid dataframe to operate on
        if current_data is None:
            # Use the full table if no specific columns exist
            current_data = self.data

        num_rows = len(current_data)

        if method == "uuid":
            return pd.Series([str(uuid.uuid4()) for _ in range(num_rows)], index=current_data.index)

        elif method == "incremental":
            return pd.Series(range(1, num_rows + 1), index=current_data.index)

        elif method == "hash":
            if not source_column:
                raise ValueError("'source_column' is required for 'hash' method.")
            if source_column not in current_data.columns:
                raise KeyError(f"Column '{source_column}' not found in source data.")

            return current_data[source_column].astype(str).apply(lambda x: hashlib.sha256(x.encode()).hexdigest())

        else:
            raise ValueError(f"Unsupported ID generation method: {method}")

    # Helper method
    def perform_lookup(self, vocabulary, code):
        """
        Perform a lookup in the specified vocabulary.
        Dummy implementation: Replace with lookup logic.
        """
        # Mock lookup logic (replace with database or API call)
        lookup_table = {"icd_to_snomed": {"I10": 316866, "E11.9": 201826}}
        return lookup_table.get(vocabulary, {}).get(code, None)
