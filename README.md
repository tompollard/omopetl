# OMOPETL

Package for creating OMOP ETLs.

## Structure

1. `omopetl` package contains all the reusable ETL logic
2. Project structure contains project-specific configurations, mappings, and input/output data.

## Usage

### Initializing a transformation project

When a user runs `omopetl startproject myproject`, the following folder structure is created:

```
myproject/
├── config/
│   ├── etl_config.yaml      # ETL configuration
│   ├── mappings.yaml        # Column mappings and transformations
│   ├── source_schema.yaml   # Source schema
│   ├── target_schema.yaml   # Target schema (OMOP)
├── data/
│   ├── source/              # Input CSV files
│   ├── target/              # Output CSV files (OMOP)
```

### Defining the transformation

To run a transformation project (i.e. to convert source data to target data) it is first necessary to update the yaml files in the `config` folder. For an example configuration, you can start a new demo project with:

```
omopetl startdemo mydemo
```

See the "Transformations` section below for a list of transformations that can be applied.

### Running a transformation

Once your project is configured, you can run transform the data in your source folder (`./myproject/data/source/*`) with the following command:

```
omopetl run myproject --dry
```

The `--dry` argument does a dry run only, meaning the code is run but the files are not saved. Remove the `--dry` argument to run the full transformation.

## Transformations

1. Direct Column Mapping: Map a column from the source table directly to a column in the target table without modification.

Example: Mapping subject_id in patients to person_id in PERSON.

```
- source_column: subject_id
  target_column: person_id
```

2. Value Mapping: Transform specific values in a column to standardized values, such as OMOP concept IDs.

Example: Mapping gender values M and F in patients to OMOP concept IDs 8507 (male) and 8532 (female).

```
- source_column: gender
  target_column: gender_concept_id
  transformation:
    type: map
    values:
      M: 8507
      F: 8532
```

3. Lookups: Map source codes (e.g., ICD-9/ICD-10 codes) to OMOP concept IDs using a vocabulary or lookup table.

Example: Mapping ICD codes in diagnoses_icd to OMOP standard concept IDs.

```
- source_column: icd_code
  target_column: condition_concept_id
  transformation:
    type: lookup
    vocabulary: icd_to_snomed
```

4. Date Normalization: Format or extract parts of dates (e.g., year, month, day) from source columns.

Example: Extracting year_of_birth from the dob column in patients.

```
- source_column: dob
  target_column: birth_datetime
  transformation:
    type: normalize_date
    format: "%Y-%m-%d"
```

5. Aggregation: Combine multiple rows or columns to calculate summary values (e.g., min, max, sum).

Example: Aggregating multiple labevents rows for the same patient and time window into a single measurement.

```
- source_column: value
  target_column: aggregated_value
  transformation:
    type: aggregate
    method: sum
    group_by: [subject_id, charttime]
```

6. Row Filtering: Include or exclude rows based on conditions.

Example: Exclude diagnoses_icd rows with icd_version = 10.

```
- source_column: icd_version
  target_column: condition_concept_id
  transformation:
    type: filter
    condition: "icd_version == 9"
```

7. Concatenation: Concatenate multiple columns into a single column, often used for generating unique identifiers.

Example: Concatenating subject_id and stay_id to form visit_detail_id.

```
- source_columns: [subject_id, stay_id]
  target_column: visit_detail_id
  transformation:
    type: concatenate
    separator: "-"
```

8. Default Values: Assign a default value to a column when the source column is missing or null.

Example: Assigning a default concept ID for missing admission_type.

```
- target_column: visit_concept_id
  transformation:
    type: default
    value: 44818518
```

9. Multi-Table Merging: Combine data from multiple source tables into a single target table.

Example: Combining admissions and transfers into VISIT_OCCURRENCE.

```
- source_columns: [admissions.hadm_id, transfers.hadm_id]
  target_column: visit_occurrence_id
  transformation:
    type: merge
    merge_key: hadm_id
```

10. Conditional Transformations: Apply transformations based on conditions in the source data.

Example: Assigning different visit_concept_id values based on admission_type.

```
- source_column: admission_type
  target_column: visit_concept_id
  transformation:
    type: conditional_map
    conditions:
      - condition: "admission_type == 'EMERGENCY'"
        value: 9203
      - condition: "admission_type == 'ELECTIVE'"
        value: 9201
```

11. Derived Columns: Calculate new columns from existing data (e.g., differences between dates).

Example: Calculating length_of_stay as the difference between dischtime and admittime.

```
- source_columns: [admittime, dischtime]
  target_column: length_of_stay
  transformation:
    type: derive
    formula: "dischtime - admittime"
```

12. Splitting Columns: Split a single source column into multiple target columns.

Example: Splitting dob into year_of_birth, month_of_birth, and day_of_birth.

```
- source_column: dob
  target_columns:
    - year_of_birth
    - month_of_birth
    - day_of_birth
  transformation:
    type: split_date
```

13. Multi-Step Transformations: Apply a sequence of transformations to a single column.

Example: Normalize a date and then filter rows based on the normalized value.

```
- source_column: admittime
  target_column: visit_start_datetime
  transformations:
    - type: normalize_date
      format: "%Y-%m-%d"
    - type: filter
      condition: "visit_start_datetime >= '2020-01-01'"
```
