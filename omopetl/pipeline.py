import os
import pandas as pd
from omopetl.transform import Transformer
from omopetl.utils import load_yaml


def extract_and_combine_data(config, source_tables, project_path, source_schema):
    """
    Extract data from multiple source tables and combine them.

    Parameters:
    - config: The ETL configuration.
    - source_tables: List of source tables to extract data from.
    - project_path: The project folder path.
    - source_schema: The source schema definition for validation.

    Returns:
    - DataFrame: Combined data from all source tables.
    """
    combined_data = pd.DataFrame()
    source_dir = os.path.join(project_path, config['etl']['source']['directory'])

    for source_table in source_tables:
        file_path = os.path.join(source_dir, f"{source_table}.csv")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source file not found: {file_path}")

        # extract data
        data = pd.read_csv(file_path)

        # validate schema
        validate_schema(data, source_schema, source_table)

        # combine data
        combined_data = pd.concat([combined_data, data], ignore_index=True)

    return combined_data


def load_data(config, data, target_table, project_path):
    """
    Load data into the target, resolving paths relative to the project path.

    Parameters:
    - config: The ETL configuration.
    - data: Transformed DataFrame.
    - target_table: Name of the target table.
    - project_path: The project folder path.
    """
    target_type = config['etl']['target']['type']
    target_dir = os.path.join(project_path, config['etl']['target']['directory'])

    if target_type == 'csv':
        os.makedirs(target_dir, exist_ok=True)
        output_path = os.path.join(target_dir, f"{target_table}.csv")
        data.to_csv(output_path, index=False)
    else:
        raise ValueError(f"Unsupported target type: {target_type}")


def validate_schema(data, schema, table_name):
    """
    Validate a DataFrame against the schema.

    Parameters:
    - data: pd.DataFrame to validate.
    - schema: Dictionary of the schema definition.
    - table_name: Name of the table to validate.

    Raises:
    - ValueError: If the schema validation fails.
    """
    expected_columns = schema.get(table_name, {}).get("columns", {})
    actual_columns = data.columns

    missing_columns = [col for col in expected_columns if col not in actual_columns]
    if missing_columns:
        raise ValueError(f"Missing columns in {table_name}: {missing_columns}")

    extra_columns = [col for col in actual_columns if col not in expected_columns]
    if extra_columns:
        print(f"Warning: Extra columns in {table_name}: {extra_columns}")


def validate_data(data, validation_rules, table_name):
    """
    Validate data against rules from validation.yaml.

    Parameters:
    - data: DataFrame to validate.
    - validation_rules: Validation rules for the table.
    - table_name: Name of the table being validated.

    Raises:
    - ValueError: If any validation rule is violated.
    """
    pass


def run_etl(project_path, dry=False):
    """
    Main ETL pipeline.

    Parameters:
    - project_path: Path to the project folder.
    - dry: If True, run the ETL without saving the output (dry run).
    """
    # Load configurations
    config_path = os.path.join(project_path, "config", "etl_config.yaml")
    mappings_path = os.path.join(project_path, "config", "mappings.yaml")
    source_schema_path = os.path.join(project_path, "config", "source_schema.yaml")
    target_schema_path = os.path.join(project_path, "config", "target_schema.yaml")

    etl_config = load_yaml(config_path)
    mappings = load_yaml(mappings_path)
    source_schema = load_yaml(source_schema_path)
    target_schema = load_yaml(target_schema_path)

    for table_config in etl_config['etl']['mappings']:
        # Support multiple source tables
        source_tables = table_config['source_tables']
        target_table = table_config['target_table']
        column_mappings = mappings[table_config['column_mappings']]

        print(f"Processing tables: {', '.join(source_tables)} -> {target_table}")

        # Extract and combine data
        data = extract_and_combine_data(etl_config, source_tables,
                                        project_path, source_schema)

        # Transform
        transformer = Transformer(data)
        transformed_data = transformer.apply_transformations(column_mappings)

        # Validate transformed data against target schema
        validate_schema(transformed_data, target_schema, target_table)

        # Load
        if not dry:
            load_data(etl_config, transformed_data, target_table, project_path)
            print(f"Saved transformed data for table: {target_table}")
        else:
            print(f"Dry run: Data for table '{target_table}' not saved.")
