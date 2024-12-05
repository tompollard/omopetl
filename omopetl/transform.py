import os
import uuid
import pandas as pd
from datetime import datetime


class Transformer:
    def __init__(self, data, project_path):
        """
        Initialize the Transformer with source data and project path.

        Parameters:
        - data: DataFrame containing the source data.
        - project_path: Path to the project directory.
        """
        self.data = data
        self.project_path = project_path

    def apply_transformations(self, columns):
        """
        Apply transformations based on column mappings.

        Parameters:
        - columns: List of mappings with transformation details.

        Returns:
        - DataFrame: Transformed data with only the specified columns.
        """
        transformed_data = pd.DataFrame()

        for column in columns:
            target_column = column["target_column"]
            transformation = column.get("transformation")
            transform_type = transformation["type"]

            # Load the transformation method
            method = getattr(self, f"transform_{transform_type}", None)
            if method is None:
                raise ValueError(f"Unsupported transformation type: {transform_type}")

            # Call the transformation method
            source_column = column.get("source_column")
            transformed_data[target_column] = method(source_column, target_column, transformation)

        # Validate relationships
        self._validate_relationships(transformed_data)

        return transformed_data

    def _validate_relationships(self, transformed_data):
        """
        Validate source and target rows.
        """
        # Check alignment of rows against original data
        if len(self.data) != len(transformed_data):
            raise ValueError("Row count mismatch after transformations. Relationships may be broken.")

    # Transformation methods
    def transform_map(self, source_column, target_column, transformation):
        """Map values in the source column to new values."""
        value_map = transformation["values"]
        return self.data[source_column].map(value_map)

    # Transformation methods
    def transform_copy(self, source_column, target_column, transformation):
        """Copy values from the source column."""
        return self.data[source_column]

    def transform_link(self, source_column, target_column, transformation):
        """
        Handle linked table transformation with optional aggregation.

        Parameters:
        - source_column: The column in the linked table to use.
        - target_column: *Not used*. Overwritten by the column specified in the transform.
        - transformation: Dictionary containing transformation details.

        Returns:
        - Series: The transformed column data.
        """
        linked_table_name = transformation["linked_table"]
        link_column = transformation["link_column"]

        # Override the source_column using the version specified in the transform.
        source_column = transformation["source_column"]

        # Load the linked table
        linked_table_path = os.path.join(self.project_path, "data", "source", f"{linked_table_name}.csv")
        if not os.path.exists(linked_table_path):
            raise FileNotFoundError(f"Linked table not found: {linked_table_path}")

        linked_table = pd.read_csv(linked_table_path)

        # Handle aggregation if specified
        aggregation = transformation.get("aggregation")
        if aggregation:
            method = aggregation.get("method", "first")
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

    def transform_lookup(self, source_column, target_column, transformation):
        """Perform a lookup transformation using a vocabulary."""
        vocabulary = transformation["vocabulary"]
        return self.data[source_column].apply(lambda x: self.perform_lookup(vocabulary, x)).astype("Int64")

    def transform_normalize_date(self, source_column, target_column, transformation):
        """Normalize date values to a specific format."""
        date_format = transformation.get("format", "%Y-%m-%d")
        return pd.to_datetime(self.data[source_column], errors="coerce").dt.strftime(date_format)

    def transform_compare_deathtime(self, source_column, target_column, transformation):
        """Verify if deathtime is before or equal to dischtime to set death_date and death_datetime"""
        source_column1 = transformation["source_column_1"]
        source_column2 = transformation["source_column_2"]
        source_column1 = pd.to_datetime(self.data[source_column1], format="%Y-%m-%d %H:%M:%S")
        source_column2 = pd.to_datetime(self.data[source_column2], format="%Y-%m-%d %H:%M:%S")
        if source_column1 is not None and source_column1.tolist() <= source_column2.tolist():
            if target_column == "death_date":
                target = source_column1.dt.date
            else :
                target = source_column1
        else:
            if target_column == "death_date":
                target = source_column2.dt.date
            else :
                target = source_column2
        return target

    def transform_aggregate(self, source_column, target_column, transformation):
        """Aggregate values based on a group_by condition."""
        group_by = transformation["group_by"]
        method = transformation["method"]

        # Perform aggregation
        aggregated = self.data.groupby(group_by).agg({source_column: method}).reset_index()
        aggregated.rename(columns={source_column: target_column}, inplace=True)

        # Merge the aggregated results back into the main DataFrame
        self.data = pd.merge(self.data, aggregated, on=group_by, how="left")

        # Return the target column to allow integration in the pipeline
        return self.data[target_column]

    def transform_concatenate(self, source_columns, target_column, transformation):
        """
        Concatenate multiple columns into a single column.

        Parameters:
        - source_columns: List of columns to concatenate.
        - target_column: The resulting target column name.
        - transformation: Dictionary containing transformation details.

        Returns:
        - Series: Concatenated column as a pandas Series.
        """
        if not source_columns:
            raise KeyError("source_columns is required for concatenate transformation.")
        separator = transformation.get("separator", "-")
        return self.data[source_columns].astype(str).agg(separator.join, axis=1)

    def transform_default(self, source_column, target_column, transformation):
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

    def transform_conditional_map(self, source_column, target_column, transformation):
        """Apply conditional mappings."""
        conditions = transformation["conditions"]
        result_column = pd.Series(index=self.data.index, dtype="Int64")
        for condition in conditions:
            condition_str = condition["condition"]
            value = condition["value"]
            result_column[self.data.eval(condition_str)] = value
        return result_column

    def transform_derive(self, source_column, target_column, transformation):
        """Calculate derived values using a formula."""
        formula = transformation["formula"]
        return self.data.eval(formula)

    def transform_split_date(self, source_column, target_column, transformation):
        """Split date values into year, month, and day."""
        date_parts = ["year", "month", "day"]
        for part in date_parts:
            if f"{part}_of_birth" in transformation["target_columns"]:
                self.data[f"{part}_of_birth"] = pd.to_datetime(self.data[source_column]).dt.__getattribute__(part)
        return self.data

    def transform_generate_id(self, source_column, target_column, transformation):
        """Generate a universal unique identifier for each row in the source column."""
        return [str(uuid.uuid4()) for _ in range(len(self.data))]

    # Helper method
    def perform_lookup(self, vocabulary, code):
        """
        Perform a lookup in the specified vocabulary.
        Dummy implementation: Replace with lookup logic.
        """
        # Mock lookup logic (replace with database or API call)
        lookup_table = {"icd_to_snomed": {"I10": 316866, "E11.9": 201826}}
        return lookup_table.get(vocabulary, {}).get(code, None)
