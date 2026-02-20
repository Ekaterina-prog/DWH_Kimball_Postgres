Architecture Overview: DWH (Kimball, PostgreSQL)

1. Source Layer
Source system: PostgreSQL database dvdrental.

Connection is implemented via postgres_fdw.
Source tables are imported into schema film_src.

The Source layer contains no business transformations — it reflects the operational system structure as-is.

2. Staging Layer
Schema: staging

Purpose
- Intermediate storage
- Minimal technical transformation
- Preparation for Core loading
- Incremental data processing

Key Characteristics
- Full load for small reference tables
- Incremental load for inventory, rental, payment
- Soft delete support (deleted column)
- Load control via staging.last_update

Centralized Load Time Control
- staging.get_last_update_table(table_name)
- staging.set_table_load_time(table_name, current_update_dt)

The staging layer:
- Does not contain surrogate keys
- Does not implement business logic
- Mirrors source structure with technical extensions (last_update, deleted)

Implemented Tables
film, inventory, rental, payment, staff, address, city, store

3. Core Layer (Star Schema – Kimball)
Schema: core

A dimensional model implemented using Kimball methodology.
Facts reference dimensions via surrogate keys.

Dimensions
dim_date
Calendar dimension generated via core.load_date.

Includes:
- Day, month, quarter, year
- ISO week, week_of_year
- First/last day of period
- Weekend indicator

dim_inventory (SCD Type 2)
Implements Slowly Changing Dimension Type 2.

Historical fields:
- effective_date_from
- effective_date_to
- is_active

Denormalized attributes from film:
- title
- rental_duration
- rental_rate
- length
- rating
Any change in film attributes creates a new dimension version, ensuring historical correctness of facts.

dim_staff
Fully reloaded dimension (no SCD logic).

4. Fact Tables
fact_rental
Transaction-level fact table.

Contains:
- rental_id
- inventory_fk
- staff_fk
- rental_date_fk
- return_date_fk

Implements SCD Type 2.
New version is created when:
- staff_id
- inventory_id
- rental_date
- return_date (if already populated and changed)
If return_date was NULL and becomes populated for the first time, an UPDATE is performed without versioning.

fact_payment
Contains:
- payment_id
- amount
- rental_id
- payment_date_fk
- inventory_fk
- staff_fk

Implements:
- Incremental loading
- Soft delete handling
- SCD Type 2 (business-event auditing)

Versioning is triggered by changes in:
-amount
-rental_id
- payment_date
Changes in surrogate keys only trigger UPDATE (no new version).
Historical logic is applied to preserve business-event auditability.

5. Data Mart Layer
Schema: report

Aggregated fact tables:
- report.sales_date
- report.sales_film

Procedures:
- report.sales_date_calc()
-report.sales_film_calc()

Purpose:
- Performance optimization
- Reduced load on transactional facts
- Pre-calculated metrics

6. Loading Strategy
Hybrid strategy:

Incremental Load
- staging.inventory
- staging.rental
- staging.payment

SCD Type 2
- core.dim_inventory
- core.fact_rental
- core.fact_payment

Full Reload
- dim_staff
- film (reference)
Calendar dimension generated separately.

Unified Load Timestamp
full_load() uses a single timestamp:

declare current_update_dt timestamp = now();

This timestamp is passed to all staging procedures to ensure consistency and eliminate data desynchronization.

7. Architectural Value
The implemented solution provides:
- Incremental processing without full fact truncation
- Soft delete support
- Historical tracking in dimensions and facts
- Correct linkage between fact and dimension versions
- Business-event auditability
- Reproducible reporting snapshots
- Improved scalability through reduced full reload operations

8. Fact Table Types

Transaction Fact Tables
- fact_rental
- fact_payment

Characteristics:
- One row per business event
- Maximum granularity
- Fully additive metrics (amount)

Accumulating Snapshot (Partially Implemented)
fact_rental tracks lifecycle updates (e.g., return_date), enabling:
- Rental duration analysis
- Delay tracking
- Process stage monitoring

Aggregate Fact Tables
- report.sales_date
- report.sales_film

Used for:
- Faster reporting
- Pre-aggregated analytics
- Reduced workload on transactional facts

Semi-Additive and Non-Additive Metrics
Conceptual examples:
- Inventory balances → semi-additive
- Ratios and percentages → non-additive
The project demonstrates understanding of different fact behaviors and their architectural implications.

Further Learning: Inmon Architecture (In Progress)
After completing the Kimball implementation, the project continues with the Inmon enterprise data warehouse approach.

Currently Implemented
- Staging layer
- ODS layer (current operational state)
- REF layer (surrogate key mapping)
- Transactional ETL process

Planned Next
- Integration layer
- DDS (historical detailed storage)
- Data marts built on top of DDS
- Architectural comparison: Kimball vs Inmon













