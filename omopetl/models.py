from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
import yaml
from omopetl.logger import log_info, log_error

Base = declarative_base()


def create_model(table_name, schema):
    """Create a SQLAlchemy model dynamically from schema."""
    attributes = {
        '__tablename__': schema['table_name'],
        '__table_args__': {'extend_existing': True},
    }

    log_info(f"Creating model for table: {table_name}")

    try:
        for column_name, column_properties in schema['columns'].items():
            column_type = eval(column_properties['type'])
            attributes[column_name] = Column(column_type, primary_key=column_properties.get('primary_key', False))
        log_info(f"Model for table {table_name} created successfully.")
    except Exception as e:
        log_error(f"Error creating model for table {table_name}: {e}")
        raise

    return type(table_name.capitalize(), (Base,), attributes)


def load_models(schema_file):
    """Load models from a schema YAML file."""
    log_info(f"Loading schema from file: {schema_file}")

    try:
        with open(schema_file, 'r') as f:
            schema = yaml.safe_load(f)
        log_info(f"Schema loaded successfully from {schema_file}.")
    except Exception as e:
        log_error(f"Error loading schema from {schema_file}: {e}")
        raise

    models = {}
    for table_name, table_schema in schema.items():
        try:
            models[table_name] = create_model(table_name, table_schema)
            log_info(f"Model for table {table_name} loaded successfully.")
        except Exception as e:
            log_error(f"Error loading model for table {table_name}: {e}")
            continue

    return models
