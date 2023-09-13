{{
  config(
    target_database = dev_db,
    target_schema = dbt_imdakota2575gmailcom,
    strategy='check',
    unique_key='product_id',
    check_cols=['inventory'],
   )
}}

select * from stg_postgres__products