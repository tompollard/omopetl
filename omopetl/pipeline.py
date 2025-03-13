import os
import pandas as pd
from omopetl.transform import Transformer
from omopetl.utils import load_yaml


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
        print(f"Warning: Columns present in {table_name} table, but not schema file: {extra_columns}")


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


def run_etl(project_path, dry=False, casual=False):
    """
    Main ETL pipeline.

    Parameters:
    - project_path: Path to the project folder.
    - dry: If True, run the ETL without saving the output (dry run).
    - casual: If True, relax validation rules and warnings.
    """
    # Load configurations
    strict = not casual
    config_path = os.path.join(project_path, "config", "etl_config.yaml")
    mappings_path = os.path.join(project_path, "config", "mappings.yaml")
    source_dir = os.path.join(project_path, "data", "source")
    target_dir = os.path.join(project_path, "data", "target")

    etl_config = load_yaml(config_path)
    mappings = load_yaml(mappings_path)
    source_schema = load_yaml(os.path.join(project_path, "config", "source_schema.yaml"))
    target_schema = load_yaml(os.path.join(project_path, "config", "target_schema.yaml"))

    for mapping_name in etl_config['etl']['mappings']:

        mapping_config = mappings[mapping_name]
        source_table = mapping_config['source_table']
        target_table = mapping_config['target_table']
        transformations = mapping_config['sequence']

        print(f"Mapping {source_table} -> {target_table}")

        # Load source or target table
        # TODO: add flag to skip validation of staging tables
        if os.path.exists(os.path.join(source_dir, f"{source_table}.csv")):
            data = pd.read_csv(os.path.join(source_dir, f"{source_table}.csv"))
        elif os.path.exists(os.path.join(target_dir, f"{source_table}.csv")):
            data = pd.read_csv(os.path.join(target_dir, f"{source_table}.csv"))
        else:
            msg = f"Table '{source_table}' not found in source or target folders."
            raise FileNotFoundError(msg)

        # Validate source data schema
        if strict:
            validate_schema(data, source_schema, source_table)

        # Apply transformations
        transformer = Transformer(data, project_path, source_schema, target_schema, target_table)
        transformed_data = transformer.apply_transformations(transformations, strict)

        # Validate and save transformed data
        if strict:
            validate_schema(transformed_data, target_schema, target_table)

        # Load
        if not dry:
            load_data(etl_config, transformed_data, target_table, project_path)
            print(f"Saved transformed data for table: {target_table}")
        else:
            print(f"Dry run: Data for table '{target_table}' not saved.")
