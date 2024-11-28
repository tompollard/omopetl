import pandas as pd
import uuid


class Transformer:
    def __init__(self, data):
        """
        Initialize the Transformer with source data.

        Parameters:
        - data: DataFrame containing the source data.
        """
        self.data = data

    def apply_transformations(self, column_mappings):
        """
        Apply transformations based on column mappings.

        Parameters:
        - column_mappings: List of mappings with transformation details.

        Returns:
        - DataFrame: Transformed data with only the specified columns.
        """
        transformed_data = pd.DataFrame()  # Start with an empty DataFrame

        for mapping in column_mappings:
            source_column = mapping.get("source_column")
            source_columns = mapping.get("source_columns")
            target_column = mapping["target_column"]
            transformation = mapping.get("transformation")

            if 'transform' in mapping and mapping['transform'] == 'generate_person_id':
                transformed_data[target_column] = self.data[source_column].apply(generate_person_id)
            
            else:
                # If no transformation, just copy the column
                transformed_data[target_column] = self.data[source_column]

            # Handle direct column mapping
            if not transformation:
                if source_column and target_column:
                    transformed_data[target_column] = self.data[source_column]
                continue

            transform_type = transformation["type"]
            method = getattr(self, f"transform_{transform_type}", None)

            if method is None:
                raise ValueError(f"Unsupported transformation type: {transform_type}")

            if transform_type == "concatenate":
                # Pass source_columns for concatenate transformation
                if not source_columns:
                    raise KeyError("source_columns is required for concatenate transformation.")
                transformed_column = method(source_columns, target_column, transformation)
            elif transform_type == 'coalesce' and source_columns:
                transformed_data[target_column] = self.data[source_columns].apply(lambda row: coalesce(*row), axis=1)
            else:
                # Pass source_column for other transformations
                transformed_column = method(source_column, target_column, transformation)

            # Add the transformed column to the transformed DataFrame
            if target_column and transformed_column is not None:
                transformed_data[target_column] = transformed_column

        return transformed_data

    # Transformation methods
    def transform_map(self, source_column, target_column, transformation):
        """Map values in the source column to new values."""
        value_map = transformation["values"]
        return self.data[source_column].map(value_map).astype("Int64")

    def transform_lookup(self, source_column, target_column, transformation):
        """Perform a lookup transformation using a vocabulary."""
        vocabulary = transformation["vocabulary"]
        return self.data[source_column].apply(lambda x: self.perform_lookup(vocabulary, x)).astype("Int64")

    def transform_normalize_date(self, source_column, target_column, transformation):
        """Normalize date values to a specific format."""
        date_format = transformation.get("format", "%Y-%m-%d")
        return pd.to_datetime(self.data[source_column], errors="coerce").dt.strftime(date_format)

    def transform_generate_id(self, source_column, target_column, transformation):
        """Generate a universal unique identifier for each row in the source column."""
        self.data[target_column] = [uuid.uuid4() for _ in range(len(self.data[source_column]))]
        return self.data[target_column]
    
    def transform_filter(self, source_column, target_column, transformation):
        """
        Filter rows based on a condition.

        Parameters:
        - source_column: Not applicable for filtering.
        - target_column: Not applicable for filtering.
        - transformation: Transformation details including condition.

        Returns:
        - None: The filtering operation modifies self.data in place.
        """
        condition = transformation["condition"]

        # Apply the condition to filter rows in the DataFrame
        self.data = self.data.query(condition).copy()

        # No return; filtering modifies self.data directly
        return None

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
        """Assign a default value."""
        return transformation["value"]

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

    # Helper method
    def perform_lookup(self, vocabulary, code):
        """
        Perform a lookup in the specified vocabulary.
        Dummy implementation: Replace with lookup logic.
        """
        # Mock lookup logic (replace with database or API call)
        lookup_table = {"icd_to_snomed": {"I10": 316866, "E11.9": 201826}}
        return lookup_table.get(vocabulary, {}).get(code, None)
