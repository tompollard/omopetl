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
def transformer(sample_data):
    """Fixture to initialize the Transformer with sample data."""
    return Transformer(sample_data)


def test_direct_mapping(transformer):
    column_mappings = [
        {"source_column": "gender", "target_column": "gender_target"}
    ]
    transformed_data = transformer.apply_transformations(column_mappings)
    assert transformed_data["gender_target"].equals(transformer.data["gender"])


def test_value_mapping(transformer):
    column_mappings = [
        {
            "source_column": "gender",
            "target_column": "gender_concept_id",
            "transformation": {
                "type": "map",
                "values": {"M": 8507, "F": 8532},
            },
        }
    ]
    transformed_data = transformer.apply_transformations(column_mappings)
    expected = pd.Series([8507, 8532, pd.NA], dtype="Int64")
    assert transformed_data["gender_concept_id"].equals(expected)


def test_lookup(transformer):
    column_mappings = [
        {
            "source_column": "icd_code",
            "target_column": "condition_concept_id",
            "transformation": {
                "type": "lookup",
                "vocabulary": "icd_to_snomed",
            },
        }
    ]
    transformed_data = transformer.apply_transformations(column_mappings)
    assert transformed_data["condition_concept_id"].tolist() == [316866, 201826, pd.NA]


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
    transformed_data = transformer.apply_transformations(column_mappings)
    assert transformed_data["birth_datetime"].tolist() == ["1980-01-01", "1990-05-20", "2000-07-15"]


def test_generate_id(transformer):
    column_mappings = [
        {
            "target_column": "person_id",
            "transformation": {
                "type": "generate_id"
            },
        }
    ]
    transformed_data = transformer.apply_transformations(column_mappings)
    assert transformed_data.tolist() is not None
    assert len(transformed_data) > 0


def test_aggregate(transformer):
    column_mappings = [
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
    transformer.apply_transformations(column_mappings)

    # Assert that the aggregated values are correctly merged
    aggregated_data = transformer.data
    assert aggregated_data["charttime"].tolist() == ["2023-01-01", "2023-01-01", "2023-01-02"]
    assert aggregated_data["aggregated_value"].tolist() == [4.0, 4.0, 3.5]


def test_concatenate(transformer):
    column_mappings = [
        {
            "source_columns": ["subject_id", "gender"],
            "target_column": "subject_gender_id",
            "transformation": {
                "type": "concatenate",
                "separator": "-",
            },
        }
    ]
    transformed_data = transformer.apply_transformations(column_mappings)

    # Assert that concatenated column is correctly added
    assert transformed_data["subject_gender_id"].tolist() == ["1-M", "2-F", "3-None"]


def test_default(transformer):
    column_mappings = [
        {
            "source_column": None,
            "target_column": "default_value_column",
            "transformation": {
                "type": "default",
                "value": 42,
            },
        }
    ]
    transformed_data = transformer.apply_transformations(column_mappings)
    assert (transformed_data["default_value_column"] == 42).all()


def test_conditional_map(transformer):
    column_mappings = [
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
    transformed_data = transformer.apply_transformations(column_mappings)
    assert transformed_data["conditional_gender_id"].tolist() == [8507, 8532, pd.NA]


def test_derive(transformer):
    column_mappings = [
        {
            "source_column": None,
            "target_column": "derived_column",
            "transformation": {
                "type": "derive",
                "formula": "value * 2",
            },
        }
    ]
    transformed_data = transformer.apply_transformations(column_mappings)
    assert transformed_data["derived_column"].tolist() == [3.0, 5.0, 7.0]
