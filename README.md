# OMOPETL

Package for creating OMOP ETLs.

## Structure

1. `omopetl` package contains all the reusable ETL logic
2. Project structure contains project-specific configurations, mappings, and input/output data.

## Usage

When a user runs `omopetl startproject myproject`, the following folder structure is created:

```
myproject/
├── config/
│   ├── etl_config.yaml      # ETL configuration
│   ├── mappings.yaml        # Column mappings and transformations
│   ├── source_schema.yaml    # Source schema
│   ├── target_schema.yaml     # Target schema (OMOP)
├── data/
│   ├── source/               # Input CSV files
│   ├── target/                # Output CSV files (OMOP)
```