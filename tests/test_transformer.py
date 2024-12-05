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
            "source_column": "gender",
            "target_column": "gender_target",
            "transformation": {
                "type": "copy",
            },
        }
    ]
    transformed_data = transformer.apply_transformations(columns)
    assert "gender_target" in transformed_data.columns
    assert transformed_data["gender_target"].equals(transformer.data["gender"])


def test_value_mapping(transformer):
    columns = [
        {
            "source_column": "gender",
            "target_column": "gender_concept_id",
            "transformation": {
                "type": "map",
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
            "source_column": "icd_code",
            "target_column": "condition_concept_id",
            "transformation": {
                "type": "lookup",
                "vocabulary": "icd_to_snomed",
            },
        }
    ]
    transformed_data = transformer.apply_transformations(columns)
    assert "condition_concept_id" in transformed_data.columns


def test_normalize_date(transformer):
    column_mappings = [
        {
            "source_column": "dob",
            "target_column": "birth_datetime",
            "transformation": {
                "type": "normalize_date",
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


def test_aggregate(transformer):
    columns = [
        {
            "source_column": "value",
            "target_column": "aggregated_value",
            "transformation": {
                "type": "aggregate",
                "group_by": ["charttime"],
                "method": "sum",
            },
        }
    ]
    transformed_data = transformer.apply_transformations(columns)
    assert "aggregated_value" in transformed_data.columns


def test_concatenate(transformer):
    columns = [
        {
            "source_column": ["subject_id", "gender"],
            "target_column": "subject_gender_id",
            "transformation": {
                "type": "concatenate",
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
            "source_column": "gender",
            "target_column": "conditional_gender_id",
            "transformation": {
                "type": "conditional_map",
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
