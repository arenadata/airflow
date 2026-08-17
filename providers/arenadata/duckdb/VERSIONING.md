# Versioning Policy

## Overview

The Arenadata DuckDB Provider follows [Semantic Versioning (SemVer)](https://semver.org/) principles.

## Version Format

Versions follow the format: `MAJOR.MINOR.PATCH` (e.g., `1.0.0`)

- **MAJOR** (X.0.0): Backward incompatible changes
- **MINOR** (0.X.0): New functionality with backward compatibility
- **PATCH** (0.0.X): Bug fixes with backward compatibility

## Initial Version

All providers start with version **1.0.0** (no alpha or beta releases).

## Version Increment Rules

| Change Type                      | Version Increment | Example                          |
|----------------------------------|-------------------|----------------------------------|
| Method/class removal             | MAJOR             | 1.0.0 → 2.0.0                   |
| Method signature change          | MAJOR             | 1.0.0 → 2.0.0                   |
| Breaking API changes             | MAJOR             | 1.0.0 → 2.0.0                   |
| New functionality (backward compatible) | MINOR      | 1.0.0 → 1.1.0                   |
| New operators/hooks/sensors      | MINOR             | 1.0.0 → 1.1.0                   |
| Bug fixes                        | PATCH             | 1.0.0 → 1.0.1                   |
| Documentation updates            | PATCH             | 1.0.0 → 1.0.1                   |
| Dependency updates (minor)       | PATCH             | 1.0.0 → 1.0.1                   |
| Dependency updates (major)       | MINOR             | 1.0.0 → 1.1.0                   |

## Examples

### MAJOR Version Change (Breaking)
```python
# Version 1.0.0
def run_query(sql: str) -> str:
    pass

# Version 2.0.0 - signature changed (breaking)
def run_query(sql: str, database: str = ":memory:") -> str:
    pass
```

### MINOR Version Change (New Feature)
```python
# Version 1.0.0
class DuckDbOperator:
    pass

# Version 1.1.0 - new sensor added
class DuckDbSqlSensor:  # New functionality
    pass
```

### PATCH Version Change (Bug Fix)
```python
# Version 1.0.0
def test_connection() -> tuple[bool, str]:
    # Bug: ignores readonly extra
    pass

# Version 1.0.1 - bug fixed
def test_connection() -> tuple[bool, str]:
    # Fixed: respects readonly extra
    pass
```

## Backward Compatibility

- **MAJOR** versions may break backward compatibility
- **MINOR** and **PATCH** versions must maintain backward compatibility
- Deprecated features should be marked with warnings before removal in next MAJOR version

## Release Process

1. Update version in `__init__.py` and `provider.yaml`
2. Update `CHANGELOG.rst` with changes
3. Create git tag with version number
4. Build and publish package
