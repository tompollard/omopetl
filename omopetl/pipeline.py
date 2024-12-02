import os
import pandas as pd
from omopetl.transform import Transformer
from omopetl.utils import load_yaml
from logger import log_info, log_warning, log_error  # Import logger

def extract_and_combine_data(config, source_tables, project_path, source_schema):
    """
    Extract data from multiple source tables and combine them.
    """
    combined_data = pd.DataFrame()
    source_dir = os.path.join(project_path, config['etl']['source']['directory'])

    log_info(f"Starting extraction of data from source tables: {', '.join(source_tables)}")

    for source_table in source_tables:
        file_path = os.path.join(source_dir, f"{source_table}.csv")
        if not os.path.exists(file_path):
            log_error(f"Source file not found: {file_path}")
            raise FileNotFoundError(f"Source file not found: {file_path}")

        log_info(f"Extracting data from {source_table}...")
        # extract data
        data = pd.read_csv(file_path)

        # validate schema
        validate_schema(data, source_schema, source_table)

        # combine data
        combined_data = pd.concat([combined_data, data], ignore_index=True)

    log_info("Data extraction and combination completed.")
    return combined_data


def load_data(config, data, target_table, project_path):
    """
    Load data into the target, resolving paths relative to the project path.
    """
    target_type = config['etl']['target']['type']
    target_dir = os.path.join(project_path, config['etl']['target']['directory'])

    log_info(f"Loading data into target table: {target_table}")

    if target_type == 'csv':
        os.makedirs(target_dir, exist_ok=True)
        output_path = os.path.join(target_dir, f"{target_table}.csv")
        data.to_csv(output_path, index=False)
        log_info(f"Data for table {target_table} saved to {output_path}")
    else:
        log_error(f"Unsupported target type: {target_type}")
        raise ValueError(f"Unsupported target type: {target_type}")


def validate_schema(data, schema, table_name):
    """
    Validate a DataFrame against the schema.
    """
    expected_columns = schema.get(table_name, {}).get("columns", {})
    actual_columns = data.columns

    log_info(f"Validating schema for table: {table_name}")

    missing_columns = [col for col in expected_columns if col not in actual_columns]
    if missing_columns:
        log_error(f"Missing columns in {table_name}: {missing_columns}")
        raise ValueError(f"Missing columns in {table_name}: {missing_columns}")

    extra_columns = [col for col in actual_columns if col not in expected_columns]
    if extra_columns:
        log_warning(f"Columns present in {table_name} but not in schema: {extra_columns}")


def validate_data(data, validation_rules, table_name):
    """
    Validate data against rules from validation.yaml.
    """
    pass  # Add your validation logic here


def run_etl(project_path, dry=False):
    """
    Main ETL pipeline.
    """
    log_info(f"Starting ETL process for project: {project_path}")
    
    # Load configurations
    config_path = os.path.join(project_path, "config", "etl_config.yaml")
    mappings_path = os.path.join(project_path, "config", "mappings.yaml")
    source_schema_path = os.path.join(project_path, "config", "source_schema.yaml")
    target_schema_path = os.path.join(project_path, "config", "target_schema.yaml")

    try:
        etl_config = load_yaml(config_path)
        mappings = load_yaml(mappings_path)
        source_schema = load_yaml(source_schema_path)
        target_schema = load_yaml(target_schema_path)
        log_info("Configuration files loaded successfully.")
    except Exception as e:
        log_error(f"Error loading configuration files: {e}")
        raise

    for table_config in etl_config['etl']['mappings']:
        # Support multiple source tables
        source_tables = table_config['source_tables']
        target_table = table_config['target_table']
        column_mappings = mappings[table_config['column_mappings']]

        log_info(f"Processing tables: {', '.join(source_tables)} -> {target_table}")

        # Extract and combine data
        try:
            data = extract_and_combine_data(etl_config, source_tables, project_path, source_schema)
        except Exception as e:
            log_error(f"Error extracting and combining data for {target_table}: {e}")
            continue  # Skip to the next table if there's an error

        # Transform
        transformer = Transformer(data)
        transformed_data = transformer.apply_transformations(column_mappings)

        # Validate transformed data against target schema
        try:
            validate_schema(transformed_data, target_schema, target_table)
        except ValueError as e:
            log_error(f"Schema validation failed for {target_table}: {e}")
            continue  # Skip to the next table if there's a validation error

        # Load
        if not dry:
            try:
                load_data(etl_config, transformed_data, target_table, project_path)
                log_info(f"Transformed data for {target_table} saved successfully.")
            except Exception as e:
                log_error(f"Error loading data for {target_table}: {e}")
        else:
            log_info(f"Dry run: Data for table '{target_table}' not saved.")

    log_info("ETL process completed.")

