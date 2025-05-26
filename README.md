# Brand Data Processing

This project processes location data for various retail brands using PySpark, following ETL (Extract, Transform, Load) principles.

## Project Structure

```
.
├── src/
│   ├── config/
│   │   ├── settings.py       # Configuration and patterns
│   │   └── spark_session.py  # Spark session management
│   ├── etl/                  # ETL pipeline components
│   │   ├── extract.py        # Data extraction
│   │   ├── transform.py      # Data transformation
│   │   └── load.py          # Data saving with GDPR compliance
│   └── utils/
│       └── logger.py         # Logging configuration
└── main.py                   # Main entry point
```

## ETL Pipeline Flow

The data processing follows a strict ETL (Extract, Transform, Load) order:

1. **Extract** (`extract.py`):
   - Validates brand inputs against allowed values
   - Processes input files for each brand
   - Normalizes schema components:
     - Converts sellingPartners to ARRAY<STRING>
     - Standardizes temporaryClosures to ARRAY<STRUCT<from: STRING, till: STRING>>
   - Combines data from multiple files and brands

2. **Transform** (`transform.py`):
   - Processes postal codes and maps provinces
   - Standardizes geographical coordinates
   - Anonymizes address information (streetName and houseNumber)
   - One-hot encodes handover services
   - Handles selling partners data

3. **Load** (`load.py`):
   - Implements GDPR-compliant data separation:
     - GDPR-sensitive data: All columns except anonymized fields (streetName, houseNumber)
     - Non-GDPR data: All columns except address
   - Partitions both datasets by brand
   - Saves as compressed parquet format
   - Optimizes for future querying

## Input/Output Examples

### Input Format
Input files may follow the pattern: `{brand}-*.json`

Example input file (`clp-places.json`):
```json
{
    "address": {
        "postal": "1000",
        "street": "Main St",
        "houseNumber": "123"
    },
    "geoCoordinates": {
        "lat": 50.8,
        "lon": 4.3
    },
    "handoverServices": ["pickup", "delivery"],
    "sellingPartners": ["partner1", "partner2"],
    "temporaryClosures": ["2024-01-01", "2024-12-31"]
}
```

### Processing Flow Example

1. **After Extract**:
```python
# Input files processed and combined:
{
    "address": {
        "postal": "1000",
        "street": "Main St",
        "houseNumber": "123"
    },
    "geoCoordinates": {"lat": 50.8, "lon": 4.3},
    "handoverServices": ["pickup", "delivery"],
    "sellingPartners": ["partner1", "partner2"],  # Normalized as ARRAY<STRING>
    "temporaryClosures": [                        # Normalized as ARRAY<STRUCT>
        {"from": "2024-01-01", "till": null}
    ],
    "brand": "clp"
}
```

2. **After Transform**:
```python
# For non-GDPR data (processed_data.parquet):
{
    "postal_code": "1000",
    "province": "Brussel",
    "lat": 50.8,
    "lon": 4.3,
    "has_handoverServices": 1,
    "has_pickup": 1,
    "has_delivery": 1,
    "brand": "clp",
    "streetName": "***",    # Anonymized
    "houseNumber": "***"    # Anonymized
}

# For GDPR-sensitive data (gdpr_sensitive_data.parquet):
{
    "address": {
        "postal": "1000",
        "street": "Main St",
        "houseNumber": "123"
    },
    "postal_code": "1000",
    "province": "Brussel",
    "lat": 50.8,
    "lon": 4.3,
    "has_handoverServices": 1,
    "has_pickup": 1,
    "has_delivery": 1,
    "sellingPartners": ["partner1", "partner2"],
    "temporaryClosures": [{"from": "2024-01-01", "till": null}],
    "brand": "clp"
}
```

3. **Final Output Structure**:
```
# GDPR-sensitive data (All columns except anonymized fields)
gdpr_sensitive_data.parquet/
├── brand=clp/
│   └── part-00000-xxx.parquet      # Contains all data 
├── brand=dats/
│   └── part-00000-xxx.parquet
├── brand=okay/
│   └── part-00000-xxx.parquet
├── brand=spar/
│   └── part-00000-xxx.parquet
└── brand=cogo/
    └── part-00000-xxx.parquet

# Non-GDPR data (All columns except address)
processed_data.parquet/
├── brand=clp/
│   └── part-00000-xxx.parquet      # Contains all data except streetName & houseNumber
├── brand=dats/
│   └── part-00000-xxx.parquet
├── brand=okay/
│   └── part-00000-xxx.parquet
├── brand=spar/
│   └── part-00000-xxx.parquet
└── brand=cogo/
    └── part-00000-xxx.parquet
```

## Usage

### Python API

```python
from main import get_data_by_brand

# Single brand - string
df = get_data_by_brand("clp")

# Multiple brands - comma-separated string
df = get_data_by_brand("clp, dats")

# Multiple brands - list
df = get_data_by_brand(["clp", "dats", "okay"])

# Multiple brands - tuple
df = get_data_by_brand(("clp", "dats"))

# Multiple brands - set (order not guaranteed)
df = get_data_by_brand({"clp", "dats"})
```

Note: Brand names are case-insensitive and duplicates are automatically removed while preserving order.

### Command Line
```bash
python main.py
```

## Features

- **Flexible Input Handling**:
  - Multiple input formats for brand specification
  - Case-insensitive brand names
  - Duplicate removal with order preservation
  - Support for multiple file formats per brand

- **Robust Data Processing**:
  - Schema normalization and validation
  - Comprehensive error handling
  - Detailed logging at all stages
  - Performance optimized operations

- **GDPR Compliance**:
  - Separate storage for sensitive and non-sensitive data
  - Address anonymization during transformation
  - Brand-based partitioning for both datasets
  - Controlled access separation

- **Performance Optimization**:
  - Brand-based partitioning for efficient querying
  - Snappy compression for both datasets
  - Optimized DataFrame operations
  - Minimal memory footprint

## Error Handling

The system includes comprehensive error handling:
- Brand name validation
- File existence checks
- Schema validation
- Data type conversion
- Detailed error logging
- Graceful failure recovery

## Logging

Multi-level logging system:
- DEBUG: Detailed processing information
- INFO: General process updates
- WARNING: Non-critical issues
- ERROR: Operation-blocking issues
- CRITICAL: Severe errors

Logs include:
- Processing statistics
- Brand-specific counts
- Schema changes
- Error details
- Performance metrics

## Configuration

### Brand Settings
```python
VALID_BRANDS = {'clp', 'okay', 'spar', 'dats', 'cogo'}
```

### Field Matching Patterns
```python
PATTERNS = {
    'latitude': r'.*lat.*|.*latitude.*',
    'longitude': r'.*lon.*|.*lng.*|.*longitude.*',
    'postal': r'.*postal.*|.*zip.*|.*postcode.*',
    'house': r'.*house.*|.*number.*',
    'street': r'.*street.*|.*road.*|.*avenue.*|.*lane.*'
}
```