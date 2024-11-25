import pandas as pd


def map_values(data, column, mapping):
    """
    Map values in a column using a provided mapping.

    Parameters:
        data (pd.DataFrame): The input data.
        column (str): The column to transform.
        mapping (dict): A dictionary mapping old values to new values.

    Returns:
        pd.DataFrame: The transformed data.
    """
    if column not in data.columns:
        raise ValueError(f"Column '{column}' not found in data.")
    data[column] = data[column].map(mapping).fillna(data[column])
    return data


def normalize_date(data, column, date_format="%Y-%m-%d"):
    """
    Normalize dates to a specific format.

    Parameters:
        data (pd.DataFrame): The input data.
        column (str): The column containing date values.
        date_format (str): The desired date format (default is '%Y-%m-%d').

    Returns:
        pd.DataFrame: The transformed data with normalized dates.
    """
    if column not in data.columns:
        raise ValueError(f"Column '{column}' not found in data.")
    data[column] = pd.to_datetime(data[column], errors='coerce').dt.strftime(date_format)
    return data


def lookup_concept(data, column, lookup_table):
    """
    Replace values in a column using a lookup table.

    Parameters:
        data (pd.DataFrame): The input data.
        column (str): The column to transform.
        lookup_table (dict): A dictionary mapping source codes to target codes.

    Returns:
        pd.DataFrame: The transformed data.
    """
    if column not in data.columns:
        raise ValueError(f"Column '{column}' not found in data.")
    data[column] = data[column].map(lookup_table)
    return data


def transform_data(data, mappings):
    """
    Apply a series of transformations to the data based on mappings.

    Parameters:
        data (pd.DataFrame): The input data.
        mappings (list): A list of column mappings with optional transformations.

    Returns:
        pd.DataFrame: The transformed data.
    """
    for mapping in mappings:
        source_col = mapping['source_column']
        target_col = mapping['target_column']

        # Apply transformations if specified
        if 'transformation' in mapping:
            transform = mapping['transformation']
            if transform['type'] == 'map':
                mapping_values = transform['values']
                data = map_values(data, source_col, mapping_values)
            elif transform['type'] == 'lookup':
                lookup_table = transform['lookup_table']
                data = lookup_concept(data, source_col, lookup_table)
            elif transform['type'] == 'normalize_date':
                data = normalize_date(data, source_col)

        # Rename the column
        data = data.rename(columns={source_col: target_col})

    return data
