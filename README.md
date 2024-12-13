# OMOPETL

The `omopetl` package has two core tools:

1. `startproject`: Create an ETL project that contains configuration files for mapping between two data structures, and
2. `run`: Run the ETL project on source data to create a set of data in the target format.

## 1. Initializing a transformation project with `startproject`

When a user runs `omopetl startproject myproject`, the following folder structure is created:

```
myproject/
├── config/
│   ├── etl_config.yaml      # ETL configuration
│   ├── mappings.yaml        # Column mappings and transformations
│   ├── source_schema.yaml   # Source schema
│   ├── target_schema.yaml   # Target schema (OMOP)
├── data/
│   ├── lookups/             # Lookup tables
│   ├── source/              # Input CSV files
│   ├── target/              # Output CSV files (OMOP)
```

After initialising the project, it is necessary to configure the transformation rules by updating the files in the `config` folder.

## 2. Running a transformation with `run`

Once your project is configured, you can run transform the data in your source folder (`./myproject/data/source/*`) with the following command:

```
omopetl run myproject --dry
```

The `--dry` argument does a dry run only, meaning the code is run but the files are not saved. Remove the `--dry` argument to run the full transformation.

## 3. Demo

For an example project, you can start a new demo project with:

```
omopetl startdemo mydemo
```

This mirrors the structure created with `startproject`. The only difference is that `config` folder is configured to transform the data in the `source` folder to OMOP.

## 4. Transformations

At the core of the `omopetl` project are the transformations. These are a set of rules that allow you to map from the source format to the target format.

**By design, transformations always return a column with the same length as the source data**. This helps to ensure row level integrity of the transformed data. If a transformation alters the length of an array, an error is raised.

Where multiple source tables map to a target table, **`omopetl` follows a "link during transformation" approach**. This involves:

- Identifying the primary table with the primary rows for the transformation.
- Specifying data to fetch from other tables.
- Defining aggregation rules to handle multiple matches (e.g. first, average, sum, etc).

## 5. Catalogue of transformations

1. Direct Column Mapping: Map a column from the source table directly to a column in the target table without modification.

    Example: Mapping subject_id in patients to person_id in PERSON.

```
- target_column: person_id
  transformation:
    type: copy
    source_column: subject_id
```

2. Value Mapping: Transform specific values in a column to standardized values, such as OMOP concept IDs.

    Example: Mapping gender values `M` and `F` in patients to OMOP concept IDs `8507` (male) and `8532` (female).

```
- target_column: gender_concept_id
  transformation:
    type: map
    source_column: gender
    values:
      M: 8507
      F: 8532
```

3. Lookups: Maps source codes (e.g., ICD-10 codes) to standard concept IDs using a lookup table.

    Example: Mapping ICD codes to SNOMED concept IDs.

```
- target_column: condition_concept_id
  transformation:
    type: lookup
    source_column: icd_code
    vocabulary: icd_to_snomed
    source_lookup_column: icd_code
    target_lookup_column: snomed_code
    default_value: 0
```

4. Date Normalization: Format or extract parts of dates (e.g., year, month, day) from source columns.

    Example: Extracting the date in `YYYY-MM-DD` format from admittime.

```
- target_column: visit_start_datetime
  transformation:
    type: normalize_date
    source_column: admittime
    format: "%Y-%m-%d"
```

5. Aggregation: Combines multiple rows or columns into summary values (e.g., `first`, `last`, `sum`) with optional ordering.

    Example:  Retrieving the earliest `visit_start_time` for a patient using `subject_id`

```
- target_column: visit_start_time
  transformation:
    type: link
    linked_table: visits
    link_column: subject_id
    source_column: visit_start_time
    aggregation:
      method: first
    order_by:
      - visit_start_time
```

6. Concatenation: Concatenate multiple columns into a single column, often used for generating unique identifiers.

    Example: Concatenating `subject_id` and `stay_id` to form visit_detail_id.

```
- target_column: visit_detail_id
  transformation:
    type: concatenate
    source_columns: [subject_id, stay_id]
    separator: "-"
```

7. Default Values: Assign a default value to a column when the source column is missing or null.

    Example: Assigning a default concept ID to `visit_concept_id` when missing.

```
- target_column: visit_concept_id
  transformation:
    type: default
    value: 44818518
```

8. Multi-Table References: Allows referencing tables in the data/target directory if they were created in previous steps.

    Example: Using the `person` table to link `patient_id`s in a subsequent mapping.

```
- target_column: patient_id
  transformation:
    type: link
    linked_table: person
    link_column: subject_id
    source_column: patient_id
    aggregation:
      method: first
```

9. Conditional Transformations: Apply transformations based on conditions in the source data.

    Example: Assigning different visit_concept_id values based on admission_type.

```
- target_column: visit_concept_id
  transformation:
    type: conditional_map
    source_column: admission_type
    conditions:
      - condition: "admission_type == 'EMERGENCY'"
        value: 9203
      - condition: "admission_type == 'ELECTIVE'"
        value: 9201
```

10. Derived Columns: Calculate new columns from existing data (e.g., differences between dates).

    Example: Calculating `length_of_stay` as the difference between dischtime and admittime.

```
- target_column: length_of_stay
  transformation:
    type: derive
    source_columns: [admittime, dischtime]
    formula: "dischtime - admittime"
```

11. Multi-Step Transformations: Applies a sequence of transformations to a single column.

    Example: Normalize admittime and filter rows based on the normalized value.

```
- target_column: visit_start_datetime
  transformations:
    - type: normalize_date
      source_column: admittime
      format: "%Y-%m-%d"
    - type: filter
      condition: "visit_start_datetime >= '2020-01-01'"
```

12. Multi-Step Transformations: Apply a sequence of transformations to a single column.

    Example: Normalize a date and then filter rows based on the normalized value.

```
- target_column: visit_start_datetime
  transformations:
    - type: normalize_date
      source_column: admittime
      format: "%Y-%m-%d"
    - type: filter
      source_column: admittime
      condition: "visit_start_datetime >= '2020-01-01'"
```

## 6. Configuring your project

After creating your new project with `omopetl startproject <PROJECTNAME>`, you will need to configure the transformation. We recommend the following approach:

1. Update `source_schema.yaml` to match your list of source tables, e.g.:

```
patients:
  table_name: patients
  columns:
    subject_id:
      type: Integer
      primary_key: true
    gender:
      type: String

admissions:
  table_name: admissions
  columns:
    hadm_id:
      type: Integer
      primary_key: true
    subject_id:
      type: Integer
    admittime:
      type: DateTime
    dischtime:
      type: DateTime
```

This file is primarily used for validating your transform (for example, making sure that relationships are maintained between variables).

**Data types should be specified in the schema files**. Supported types are currently:

- `string`
- `integer`
- `float`
- `boolean`
- `date`
- `datetime`

You can use `omopetl inferschema <PATH_TO_DATA>` to create a draft schema based on a set of CSV files. Careful though, the `primary_key` fields are likely to be incorrect, so review is needed.

2. Update `target_schema.yaml` to match your list of target tables, e.g.:

```
person:
  table_name: person
  columns:
    person_id:
      type: Integer
      primary_key: true
    gender_concept_id:
      type: Integer

visit_occurrence:
  table_name: visit_occurrence
  columns:
    visit_occurrence_id:
      type: Integer
      primary_key: true
    person_id:
      type: Integer
    visit_start_datetime:
      type: DateTime
    visit_end_datetime:
      type: DateTime
```

This file is primarily used for validating your transform (for example, making sure that relationships are maintained between variables).

**If you are creating staging tables as part of your ETL, it is important that they are also described in the target schema!**

3. Add details of your source tables and column mapping rules to `etl_config.yaml`:

```
etl:
  source:
    type: csv
    directory: ./data/source
    schema_file: ./config/source_schema.yaml

  target:
    type: csv
    directory: ./data/target
    schema_file: ./config/target_schema.yaml

# list of mappings to apply from mappings.yaml
  mappings:
    person_mapping
    visit_occurrence_mapping
```

**Don't forget to include your list of mappings in this file, including any mappings that create staging tables.**

4. Add your transformations to `mappings.yaml`:

```
person_mapping:
  - target_column: person_id
    transformation:
      type: copy
      source_column: subject_id
  - target_column: gender_concept_id
    transformation:
      type: map
      source_column: gender
      values:
        M: 8507
        F: 8532

visit_occurrence_mapping:
  - target_column: visit_occurrence_id
    transformation:
      type: copy
      source_column: hadm_id
  - target_column: person_id
    transformation:
      type: copy
      source_column: subject_id
  - target_column: visit_start_datetime
    transformation:
      type: copy
      source_column: admittime
  - target_column: visit_end_datetime
    transformation:
      type: copy
      source_column: dischtime
```

We suggest starting out with the simple transformations first. For example, try creating a first version of your transformation using only direct mappings.

5. Add custom validation rules to `validation.yaml`. For example, if dates of birth should be within the range of `1900-01-01` and `2023-12-31`, you might choose to add the following rule:

```
validation:
  person:
    - column: birth_datetime
      rule: range
      min: "1900-01-01"
      max: "2023-12-31"
```