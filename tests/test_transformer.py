import pytest
import pandas as pd
from omopetl.transform import Transformer


@pytest.fixture
def sample_data():
    """Fixture to provide sample input data."""
    return pd.DataFrame({
        "gender": ["M", "F", None],
        "dob": ["1980-01-01", "1990-05-20", "2000-07-15"],
        "icd_code": ["I10", "E11.9", "J45"],
        "admittime": ["2023-01-01 12:00:00", "2023-01-02 14:00:00", "2023-01-03 16:00:00"],
        "value": [1.5, 2.5, 3.5],
        "subject_id": [1, 2, 3],
        "charttime": ["2023-01-01", "2023-01-01", "2023-01-02"],
    })


@pytest.fixture
def project_path(tmp_path):
    # Create a temporary project path for testing
    return tmp_path


@pytest.fixture
def transformer(sample_data):
    """Fixture to initialize the Transformer with sample data."""
    return Transformer(sample_data, project_path)


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
    expected = transformer.data["gender"].map({"M": 8507, "F": 8532})
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
    assert transformed_data["birth_datetime"].tolist() == ["1980-01-01", "1990-05-20", "2000-07-15"]


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
