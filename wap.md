# Versioning Strategy for Data Processing

## Overview

This document outlines the versioning strategy for our data processing pipeline, which follows the Medallion Architecture pattern.

## Key Principles

1. Gold Layer: Follows SCD-1, containing only the latest active records.
2. Silver Layer: Stores historical versions (SCD-2) for auditing and manages transformations.
3. Bronze Layer: Remains a full raw archive, with no merging or overwrites.

## Medallion Architecture with Versioning

| Layer | Purpose | Versioning Strategy | Branching Strategy |
|-------|---------|---------------------|---------------------|
| Bronze (Raw Data Store) | Store every raw extraction as-is. | Append every new extraction with extraction_timestamp. | No merging. Every pull = a new snapshot. |
| Silver (Processed & Enriched) | Transform, detect changes & version data (SCD-2). | Compare previous Silver version, assign per-record versions. | Branch for validation, then merge. |
| Gold (Final Business View) | Maintain only the latest active records (SCD-1). | Overwrite older records with the latest state. | Branch for validation, then merge. |

## Detailed Layer Descriptions

### 1. Bronze Layer: Storing Every Pull as a Snapshot

#### Process
- Store full snapshots every time data is extracted, as OFAC does not provide CDC.
- Assign a new extraction timestamp to every pull.
- No records are modified or deleted.

#### Example Data in Bronze Table

| profile_id | identity_id | documented_names                                 | extraction_timestamp |
|------------|-------------|--------------------------------------------------|----------------------|
| 101        | 5001        | {"First Name": "John", "Last Name": "Doe"}       | 2024-02-01 10:00:00  |
| 101        | 5001        | {"First Name": "John", "Last Name": "Doe"}       | 2024-02-10 10:00:00  |
| 102        | 5002        | {"First Name": "Jane", "Last Name": "Smith"}     | 2024-02-10 10:00:00  |

#### Benefits
- Ensures full history tracking.
- Allows Silver to compare past extractions.
- Avoids unnecessary updates by storing every pull.

### 2. Silver Layer: Detecting Changes & Assigning Versions (SCD-2)

#### Process
- Extract and compare records against the previous Silver version.
- Assign versions at a record level based on changes.
- Maintain a history of changes (SCD-2) for traceability.

#### Branching Strategy
1. Create a new Silver branch from the latest Bronze snapshot.
2. Compare with previous Silver version:
   - New records: Start at Version 1.
   - Modified records: Increase version (V2, V3, etc.).
   - Unchanged records: Keep the previous version.
3. Apply transformations (Alias matching, Document validation, etc.).
4. Merge validated changes into the main Silver table.

#### Example of Silver Table (SCD-2)

| profile_id | identity_id | documented_names                                 | version | ingestion_timestamp | active_flag |
|------------|-------------|--------------------------------------------------|---------|---------------------|-------------|
| 101        | 5001        | {"First Name": "John", "Last Name": "Doe"}       | V1      | 2024-02-01          | Y           |
| 101        | 5001        | {"First Name": "John", "Last Name": "Doe"}       | V2      | 2024-02-10          | Y           |
| 102        | 5002        | {"First Name": "Jane", "Last Name": "Smith"}     | V1      | 2024-02-10          | N           |

#### Benefits
- Allows full tracking of changes.
- Ensures versioning only for changed records.
- Maintains an audit trail for governance & debugging.

### 3. Gold Layer: SCD-1 with Only the Latest Active Records

#### Process
- Gold does not track historical changes (no SCD-2).
- Overwrite previous records with the latest state.
- Only active records are retained (SCD-1).

#### Branching Strategy
1. Create a new Gold branch from the latest Silver version.
2. Apply SCD-1 logic:
   - Select only active (Y) records from Silver.
   - Overwrite the previous Gold table with the new active dataset.
   - Remove old inactive records.
3. Validate changes before merging into Gold.

#### Example of Gold Table (SCD-1)

| profile_id | unique_risk_id | alias_id  | record_status      |
|------------|----------------|-----------|---------------------|
| 101        | APP_1234       | ALIAS_001 | Active              |
| 102        | APP_5678       | ALIAS_002 | (Removed from Gold) |

#### Benefits
- Gold always represents the latest state.
- No unnecessary duplication or version tracking.
- Ensures business users see only valid active records.

## End-to-End Versioning Flow

Bronze → Silver → Gold with SCD-1 in Gold

| Layer             | What We Do                      | How We Version                                                            | Branching                            |
|-------------------|----------------------------------|---------------------------------------------------------------------------|--------------------------------------|
| Bronze (Raw)      | Store every data pull as-is.     | Assign extraction_timestamp to each pull.                                 | No merging, just append.             |
| Silver (Processed)| Transform & detect changes.      | Compare with previous Silver version, assign per-record versions (SCD-2). | Create branch, validate, merge.      |
| Gold (Final View) | Store only active records.       | Overwrite older records with the latest state (SCD-1).                    | Create branch, validate, merge.      |

## Conclusion

- Gold is always SCD-1, containing only active records.
- Silver maintains historical versions (SCD-2) for auditing.
- Bronze remains untouched, storing all raw pulls for full history.