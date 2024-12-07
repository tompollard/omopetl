from datetime import date
import pytest
import pandas as pd
from omopetl.transform import Transformer


@pytest.fixture
def sample_data():
    """Fixture to provide sample input data."""
    return pd.DataFrame({
        "gender": ["M", "F", "None"],
        "dob": ["1980-01-01", "1990-05-20", "2000-07-15"],
        "icd_code": ["I10", "E11.9", "J45"],
        "admittime": ["2023-01-01 12:00:00", "2023-01-02 14:00:00", "2023-01-03 16:00:00"],
        "value": [1.5, 2.5, 3.5],
        "subject_id": [1, 2, 3],
        "charttime": ["2023-01-01", "2023-01-01", "2023-01-02"],
    })


@pytest.fixture
def source_schema():
    """Fixture to provide a mock source schema."""
    return {
        "patients": {
            "columns": {
                "gender": {"type": "string"},
                "dob": {"type": "date"},
                "icd_code": {"type": "string"},
                "admittime": {"type": "datetime"},
                "value": {"type": "float"},
                "subject_id": {"type": "integer"},
                "charttime": {"type": "date"},
            }
        }
    }


@pytest.fixture
def target_schema():
    """Fixture to provide a mock target schema."""
    return {
        "person": {
            "columns": {
                "gender_target": {"type": "string"},
                "gender_concept_id": {"type": "integer"},
                "birth_datetime": {"type": "date"},
                "subject_gender_id": {"type": "string"},
                "default_value_column": {"type": "integer"},
                "conditional_gender_id": {"type": "integer"},
                "derived_column": {"type": "float"},
                "person_id": {"type": "string"},
                "condition_concept_id": {"type": "string"},
            }
        }
    }


@pytest.fixture
def project_path(tmp_path):
    # Create a temporary project path for testing
    return tmp_path


@pytest.fixture
def transformer(sample_data, project_path, source_schema, target_schema):
    """Fixture to initialize the Transformer with sample data."""
    return Transformer(sample_data, project_path, source_schema, target_schema, "person")


def test_direct_mapping(transformer):
    columns = [
        {
            "target_column": "gender_target",
            "transformation": {
                "type": "copy",
                "source_column": "gender",
            },
        }
    ]
    transformed_data = transformer.apply_transformations(columns)
    assert "gender_target" in transformed_data.columns
    assert transformed_data["gender_target"].equals(transformer.data["gender"])


def test_value_mapping(transformer):
    columns = [
        {
            "target_column": "gender_concept_id",
            "transformation": {
                "type": "map",
                "source_column": "gender",
                "values": {"M": 8507, "F": 8532},
            },
        }
    ]
    transformed_data = transformer.apply_transformations(columns)
    assert "gender_concept_id" in transformed_data.columns
    expected = pd.Series([8507, 8532, pd.NA], dtype="Int64", name="gender_concept_id")
    assert transformed_data["gender_concept_id"].equals(expected)


def test_lookup(transformer):
    columns = [
        {
            "target_column": "condition_concept_id",
            "transformation": {
                "type": "lookup",
                "source_column": "icd_code",
                "vocabulary": "icd_to_snomed",
            },
        }
    ]
    transformed_data = transformer.apply_transformations(columns)
    assert "condition_concept_id" in transformed_data.columns


def test_normalize_date(transformer):
    column_mappings = [
        {
            "target_column": "birth_datetime",
            "transformation": {
                "type": "normalize_date",
                "source_column": "dob",
                "format": "%Y-%m-%d",
            },
        }
    ]

    # Mock data
    transformer.data = pd.DataFrame({
        "dob": ["1980-01-01", "1990-05-20", "2000-07-15"]
    })

    transformed_data = transformer.apply_transformations(column_mappings)
    expected_dates = [date(1980, 1, 1), date(1990, 5, 20), date(2000, 7, 15)]
    assert transformed_data["birth_datetime"].tolist() == expected_dates


def test_concatenate(transformer):
    columns = [
        {
            "target_column": "subject_gender_id",
            "transformation": {
                "type": "concatenate",
                "source_columns": ["subject_id", "gender"],
                "separator": "-",
            },
        }
    ]
    transformed_data = transformer.apply_transformations(columns)
    assert "subject_gender_id" in transformed_data.columns
    expected = transformer.data[["subject_id", "gender"]].astype(str).agg("-".join, axis=1)
    assert transformed_data["subject_gender_id"].equals(expected)


def test_default(transformer):
    columns = [
        {
            "target_column": "default_value_column",
            "transformation": {
                "type": "default",
                "value": 42,
            },
        }
    ]
    transformed_data = transformer.apply_transformations(columns)
    assert "default_value_column" in transformed_data.columns
    assert (transformed_data["default_value_column"] == 42).all()


def test_conditional_map(transformer):
    columns = [
        {
            "target_column": "conditional_gender_id",
            "transformation": {
                "type": "conditional_map",
                "source_column": "gender",
                "conditions": [
                    {"condition": "gender == 'M'", "value": 8507},
                    {"condition": "gender == 'F'", "value": 8532},
                ],
            },
        }
    ]
    transformed_data = transformer.apply_transformations(columns)
    assert "conditional_gender_id" in transformed_data.columns


def test_derive(transformer):
    columns = [
        {
            "target_column": "derived_column",
            "transformation": {
                "type": "derive",
                "source_column": "value",
                "formula": "value * 2",
            },
        }
    ]
    transformed_data = transformer.apply_transformations(columns)
    assert "derived_column" in transformed_data.columns


def test_generate_id(transformer):
    columns = [
        {
            "target_column": "person_id",
            "transformation": {
                "type": "generate_id"
            },
        }
    ]
    transformed_data = transformer.apply_transformations(columns)
    assert transformed_data is not None
    assert len(transformed_data) == 3
