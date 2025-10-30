# ERP Data Warehouse Model

## Overview
This dbt project transforms the SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 tables into a proper star schema with ORDERS as the fact table and dimensions extracted from the related tables. The architecture follows a three-layer approach:
1. **Source Layer**: Original ERP data from Snowflake sample data
2. **ODS Layer**: Minimally transformed operational data store (pass-through)
3. **DW Layer**: Properly modeled dimensional warehouse with renamed columns

## Project Structure

```
models/
├── ods/
│   ├── ods__erp__customer.sql
│   ├── ods__erp__lineitem.sql
│   ├── ods__erp__nation.sql
│   ├── ods__erp__orders.sql
│   ├── ods__erp__part.sql
│   ├── ods__erp__partsupp.sql
│   ├── ods__erp__region.sql
│   └── ods__erp__supplier.sql
├── dw/
│   ├── dimensions/
│   │   ├── dim_customer.sql
│   │   ├── dim_date.sql
│   │   ├── dim_part.sql
│   │   ├── dim_supplier.sql
│   │   └── dim_location.sql
│   └── facts/
│       ├── fact_orders.sql
│       └── fact_order_items.sql
└── sources.yml
```

## Model Details

### ODS Models (Pass-through with minimal transformation)

These models simply pass through data from source to ODS with minimal transformation, maintaining original column names.

#### ods__erp__customer.sql
```sql
with source as (
    select * from {{ source('erp', 'customer') }}
)

select * from source
```

#### ods__erp__nation.sql
```sql
with source as (
    select * from {{ source('erp', 'nation') }}
)

select * from source
```

#### ods__erp__region.sql
```sql
with source as (
    select * from {{ source('erp', 'region') }}
)

select * from source
```

#### ods__erp__orders.sql
```sql
with source as (
    select * from {{ source('erp', 'orders') }}
)

select * from source
```

#### ods__erp__lineitem.sql
```sql
with source as (
    select * from {{ source('erp', 'lineitem') }}
)

select * from source
```

#### ods__erp__part.sql
```sql
with source as (
    select * from {{ source('erp', 'part') }}
)

select * from source
```

#### ods__erp__supplier.sql
```sql
with source as (
    select * from {{ source('erp', 'supplier') }}
)

select * from source
```

### DW Dimension Models

These models transform the ODS data into proper dimensional models with renamed columns.

#### dim_date.sql
```sql
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('1992-01-01')",
        end_date="to_date('1998-12-31')"
    )
    }}
),

dates as (
    select
        date_day as date_key,
        date_day as full_date,
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(dayofweek from date_day) as day_of_week,
        extract(dayofyear from date_day) as day_of_year,
        case when extract(dayofweek from date_day) in (0, 6) then true else false end as is_weekend
    from date_spine
)

select * from dates
```

#### dim_customer.sql
```sql
with customers as (
    select * from {{ ref('ods__erp__customer') }}
),

nations as (
    select * from {{ ref('ods__erp__nation') }}
),

regions as (
    select * from {{ ref('ods__erp__region') }}
),

final as (
    select
        customers.c_custkey as customer_key,
        customers.c_name as customer_name,
        customers.c_address as address,
        customers.c_phone as phone,
        customers.c_acctbal as account_balance,
        customers.c_mktsegment as market_segment,
        nations.n_nationkey as nation_key,
        nations.n_name as nation_name,
        regions.r_regionkey as region_key,
        regions.r_name as region_name
    from customers
    left join nations on customers.c_nationkey = nations.n_nationkey
    left join regions on nations.n_regionkey = regions.r_regionkey
)

select * from final
```

#### dim_location.sql
```sql
with nations as (
    select * from {{ ref('ods__erp__nation') }}
),

regions as (
    select * from {{ ref('ods__erp__region') }}
),

final as (
    select
        nations.n_nationkey as nation_key,
        nations.n_name as nation_name,
        regions.r_regionkey as region_key,
        regions.r_name as region_name
    from nations
    left join regions on nations.n_regionkey = regions.r_regionkey
)

select * from final
```

#### dim_part.sql
```sql
with parts as (
    select * from {{ ref('ods__erp__part') }}
),

final as (
    select
        p_partkey as part_key,
        p_name as part_name,
        p_mfgr as manufacturer,
        p_brand as brand,
        p_type as type,
        p_size as size,
        p_container as container,
        p_retailprice as retail_price
    from parts
)

select * from final
```

#### dim_supplier.sql
```sql
with suppliers as (
    select * from {{ ref('ods__erp__supplier') }}
),

nations as (
    select * from {{ ref('ods__erp__nation') }}
),

regions as (
    select * from {{ ref('ods__erp__region') }}
),

final as (
    select
        suppliers.s_suppkey as supplier_key,
        suppliers.s_name as supplier_name,
        suppliers.s_address as address,
        suppliers.s_phone as phone,
        suppliers.s_acctbal as account_balance,
        nations.n_nationkey as nation_key,
        nations.n_name as nation_name,
        regions.r_regionkey as region_key,
        regions.r_name as region_name
    from suppliers
    left join nations on suppliers.s_nationkey = nations.n_nationkey
    left join regions on nations.n_regionkey = regions.r_regionkey
)

select * from final
```

### DW Fact Models

These models create the fact tables with proper column naming.

#### fact_orders.sql
```sql
with orders as (
    select * from {{ ref('ods__erp__orders') }}
),

-- Create surrogate keys for date dimensions
order_dates as (
    select
        orders.*,
        to_date(o_orderdate) as order_date_key
    from orders
),

final as (
    select
        o_orderkey as order_key,
        o_custkey as customer_key,
        order_date_key,
        o_orderstatus as order_status,
        o_totalprice as total_price,
        o_orderpriority as order_priority,
        o_clerk as clerk,
        o_shippriority as ship_priority
    from order_dates
)

select * from final
```

#### fact_order_items.sql
```sql
with line_items as (
    select * from {{ ref('ods__erp__lineitem') }}
),

-- Create surrogate keys for date dimensions
line_items_with_dates as (
    select
        line_items.*,
        to_date(l_shipdate) as ship_date_key,
        to_date(l_commitdate) as commit_date_key,
        to_date(l_receiptdate) as receipt_date_key
    from line_items
),

final as (
    select
        l_orderkey as order_key,
        l_partkey as part_key,
        l_suppkey as supplier_key,
        l_linenumber as line_number,
        l_quantity as quantity,
        l_extendedprice as extended_price,
        l_discount as discount,
        l_tax as tax,
        l_returnflag as return_flag,
        l_linestatus as line_status,
        ship_date_key,
        commit_date_key,
        receipt_date_key,
        l_shipinstruct as ship_instructions,
        l_shipmode as ship_mode,
        -- Calculate derived columns
        l_extendedprice * (1 - l_discount) as discounted_price,
        l_extendedprice * (1 - l_discount) * (1 + l_tax) as final_price
    from line_items_with_dates
)

select * from final
```

## Source Configuration

Create a `sources.yml` file in your models directory:

```yaml
version: 2

sources:
  - name: erp
    database: SNOWFLAKE_SAMPLE_DATA
    schema: TPCH_SF1
    tables:
      - name: customer
      - name: lineitem
      - name: nation
      - name: orders
      - name: part
      - name: partsupp
      - name: region
      - name: supplier
```

## Documentation

Create a `schema.yml` file in your models directory:

```yaml
version: 2

models:
  - name: fact_orders
    description: "Fact table containing order header information"
    columns:
      - name: order_key
        description: "Primary key for orders"
        tests:
          - unique
          - not_null
      - name: customer_key
        description: "Foreign key to dim_customer"
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_key
      - name: order_date_key
        description: "Foreign key to dim_date"
        tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_key

  - name: fact_order_items
    description: "Fact table containing order line items"
    columns:
      - name: order_key
        description: "Part of composite primary key, foreign key to fact_orders"
        tests:
          - not_null
          - relationships:
              to: ref('fact_orders')
              field: order_key
      - name: line_number
        description: "Part of composite primary key"
        tests:
          - not_null
      - name: part_key
        description: "Foreign key to dim_part"
        tests:
          - not_null
          - relationships:
              to: ref('dim_part')
              field: part_key
      - name: supplier_key
        description: "Foreign key to dim_supplier"
        tests:
          - not_null
          - relationships:
              to: ref('dim_supplier')
              field: supplier_key

  # Additional dimension documentation would follow similar patterns
```

## Materializations Strategy

For ODS models:
```yaml
models:
  erp_dw:
    ods:
      +materialized: incremental
      +incremental_strategy: merge
      +unique_key: ["source_primary_key"]
```

For DW models:
```yaml
models:
  erp_dw:
    dw:
      +materialized: table
      dimensions:
        +tags: ["dimension"]
      facts:
        +tags: ["fact"]
```

## Performance Optimization

For large tables, consider the following optimizations:

```yaml
models:
  erp_dw:
    ods:
      ods__erp__lineitem:
        +cluster_by: ["l_orderkey"]
    dw:
      facts:
        fact_order_items:
          +cluster_by: ["order_key"]
```

## Advantages of This Architecture

1. **Clear Separation of Concerns**:
   - ODS layer: Simple pass-through with original names
   - DW layer: Business logic and column renaming

2. **Source System Traceability**:
   - ODS maintains original column names for easier debugging
   - Simplifies ETL changes when source systems change

3. **Simplified ODS Development**:
   - Drastically reduces time to create ODS models
   - Eliminates error-prone column renaming at ODS level

4. **Business Logic in One Place**:
   - All transformations happen in DW layer
   - Easier to maintain business rules

5. **Flexibility for Large Column Tables**:
   - ODS brings in all columns without manual mapping
   - DW layer can selectively include only needed columns
