with
staging_orders as (
    select * from {{ref('stg_postgres__orders')}}
),

order_number_transform as (
    select
        order_guid,
        dense_rank() over
            (order by order_placed_at_utc asc)
        as order_number
    from stg_postgres__orders
)

select * from order_number_transform