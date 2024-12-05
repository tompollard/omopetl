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
            transformations = column.get("transformations", [column.get("transformation")])

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

            # Store the final transformed column
            transformed_data[target_column] = current_data

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
    def transform_map(self, current_data, target_column, transformation):
        """Map values in the source column to new values."""
        source_column = transformation.get("source_column")
        value_map = transformation["values"]
        return current_data[source_column].map(value_map)

    # Transformation methods
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
        linked_table_name = transformation["linked_table"]
        if not linked_table_name:
            raise KeyError("'linked_table' is required for a 'link' transformation.")

        link_column = transformation["link_column"]
        if not link_column:
            raise KeyError("'link_column' is required for a 'link' transformation.")

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

    def transform_lookup(self, current_data, target_column, transformation):
        """Perform a lookup transformation using a vocabulary."""
        source_column = transformation.get("source_column")
        vocabulary = transformation["vocabulary"]
        return current_data[source_column].apply(lambda x: self.perform_lookup(vocabulary, x)).astype("Int64")

    def transform_normalize_date(self, current_data, target_column, transformation):
        """Normalize date values to a specific format."""
        source_column = transformation.get("source_column")
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

    def transform_derive(self, current_data, target_column, transformation):
        """Calculate derived values using a formula."""
        formula = transformation["formula"]
        return current_data.eval(formula)

    def transform_generate_id(self, current_data, target_column, transformation):
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
