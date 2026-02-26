Enterprise Data Warehouse Architecture (PostgreSQL)

Overview

This project implements a comprehensive Enterprise Data Warehouse in PostgreSQL using three major architectural paradigms:
- Kimball (Dimensional Modeling / Star Schema)
- Inmon (Corporate Information Factory / 3NF)
- Data Vault 2.0 (Raw Vault)

The solution demonstrates practical implementation of different DWH approaches within a single unified system.

Architecture Layers

1. Source Layer
- Source system: PostgreSQL (dvdrental)
- Connected via postgres_fdw
- No business transformations
- Pure operational data reflection

2. Staging Layer
Purpose:
- Data ingestion
- Incremental processing
- Technical preparation for core layers

Implemented features:
- Incremental load for transactional tables
- Full load for small reference tables
- Soft delete support (deleted flag)
- Centralized load timestamp control
- Unified orchestration via full_load()

Value:
- Prevents data desynchronization
- Ensures consistent load execution
- Enables scalable incremental processing

3. Core Layer – Kimball (Star Schema)

Dimensional model implementation:

Dimensions
- dim_inventory (SCD Type 2)
- dim_date
- dim_staff

Fact Tables
- fact_rental
- fact_payment

Technical solutions:
- SCD Type 2 for historical tracking
- Incremental fact loading
- Proper surrogate key management
- Aggregated reporting marts
 
Value:
- Historical accuracy of reporting
- High-performance analytical queries
- BI-ready data structure

4. Enterprise Layer – Inmon (3NF Architecture)

Implemented layers:
- ODS (Operational Data Store)
- Integration Layer (natural → surrogate key replacement)
- DDS (historical detailed storage)

Technical solutions:
- Row-level change detection using md5(row::text)
- Version closing on data change
- Enterprise-level historical tracking

Value:
- Centralized corporate data model
- Clean separation of operational and analytical structures
- Strong support for enterprise reporting

5. Data Vault 2.0 – Raw Vault

Raw Vault implementation includes:

Hubs
- Hash-based surrogate keys (MD5 of business keys)
- Business identifiers
- LoadDate / RecordSource

Links
- Hash of concatenated business keys
- Insert-only logic

Satellites
- Descriptive attributes
- Historical tracking via LoadDate / LoadEndDate
- HashDiff-based change detection

Technical characteristics:
- Insert-only architecture
- Hash-based surrogate keys
- Full historical reproducibility
- Independent scaling of entities

Value:
- High scalability
- Easy addition of new data sources
- Full auditability
- Flexible enterprise backbone

Key Technical Implementations
- Incremental loading strategy
- SCD Type 2 versioning
- Soft delete handling
- Hash-based surrogate keys (MD5)
- Insert-only Raw Vault logic
- Centralized load timestamp control
- Hybrid DWH architecture
- Power BI integration for reporting

Data Mart Layer
- Aggregated reporting tables (sales_by_date, sales_by_film)
- Pre-calculated metrics
- Reduced load on transactional facts
- BI optimization

Architectural Value

The implemented solution provides:
- Historical consistency
- Enterprise scalability
- Flexible data model extension
- Business-event auditability
- Reproducible reporting snapshots
- Reduced full reload dependency
- Clear separation of modeling paradigms

Summary

This project demonstrates practical implementation and comparison of three major DWH paradigms:
- Kimball – optimized analytical reporting
- Inmon – enterprise 3NF integration
- Data Vault 2.0 – scalable historical backbone

The architecture is designed to ensure flexibility, scalability, and full historical traceability across all layers.



