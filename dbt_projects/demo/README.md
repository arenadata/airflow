# dbt Demo Project

Demonstration dbt project for learning and testing.

## Structure

- `models/staging/` - staging layer (views), data cleaning and normalization
- `models/marts/` - marts layer (tables), business logic and aggregations
- `macros/` - reusable Jinja2 macros
- `tests/` - custom data tests
- `seeds/` - CSV files for loading
- `snapshots/` - snapshot definitions for SCD Type 2

## Commands

```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run tests
dbt test

# Run models and tests
dbt build

# Compile without execution
dbt compile

# Generate documentation
dbt docs generate
dbt docs serve
```

## Profile

Project uses Postgres profile. Configuration in `profiles.yml`.
