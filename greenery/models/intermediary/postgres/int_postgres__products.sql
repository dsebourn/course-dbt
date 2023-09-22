with

staging_products as (
    select * from {{ref('stg_postgres__products')}}
),

prep_items_ordered as (
    select * from {{ref('int_postgres__order_items_details')}}
),

prep_lifetime_stats as (
    select
        distinct items.product_guid,
        count(distinct items.order_guid) over
            (partition by items.product_guid)
            as lifetime_orders_of_product,
        sum(items.product_quantity) over
            (partition by items.product_guid)
            as lifetime_quantity_ordered,
        count(distinct items.user_guid) over
            (partition by items.product_guid)
            as lifetime_distinct_users_ordered,
        datediff(
            day,
            last_time_ordered_at_utc, 
            current_date()::timestamp_ntz
            ) as days_since_ordered_last
    from prep_items_ordered as items
),

int_transforms as (
    select
        products.product_guid,
        products.product_name,
        products.product_price,
        products.current_product_inventory,
        lifetime_stats.lifetime_orders_of_product,
        lifetime_stats.lifetime_quantity_ordered,
        lifetime_stats.lifetime_distinct_users_ordered,
        lifetime_stats.days_since_ordered_last
    from staging_products as products
    left join prep_lifetime_stats as lifetime_stats
        on products.product_guid = lifetime_stats.product_guid
)

select * from int_transforms