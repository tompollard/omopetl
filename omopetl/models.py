from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
import yaml


Base = declarative_base()


def create_model(table_name, schema):
    attributes = {
        '__tablename__': schema['table_name'],
        '__table_args__': {'extend_existing': True},
    }
    for column_name, column_properties in schema['columns'].items():
        column_type = eval(column_properties['type'])
        attributes[column_name] = Column(column_type, primary_key=column_properties.get('primary_key', False))
    return type(table_name.capitalize(), (Base,), attributes)

def load_models(schema_file):
    with open(schema_file, 'r') as f:
        schema = yaml.safe_load(f)
    return {table_name: create_model(table_name, table_schema) for table_name, table_schema in schema.items()}
