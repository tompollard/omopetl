import yaml
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_yaml(file_path, default_value=None):
    """
    Load and parse a YAML file.

    Parameters:
        file_path (str): The path to the YAML file.
        default_value (dict, optional): The value to return in case of an error (default is None).

    Returns:
        dict: Parsed YAML content as a dictionary or a default value if an error occurs.

    Raises:
        FileNotFoundError: If the file does not exist.
        yaml.YAMLError: If there is an error parsing the YAML.
    """
    try:
        logger.info(f"Loading YAML file from {file_path}")
        with open(file_path, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file)
        logger.info(f"Successfully loaded YAML file from {file_path}")
        return data

    except FileNotFoundError:
        logger.error(f"YAML file not found: {file_path}")
        if default_value is not None:
            return default_value
        raise

    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {file_path}: {e}")
        if default_value is not None:
            return default_value
        raise

    except Exception as e:
        logger.error(f"Unexpected error occurred while loading {file_path}: {e}")
        if default_value is not None:
            return default_value
        raise

    
