with
staging_order_items as (
    select * from {{ref('stg_postgres__order_items')}}
),

staging_products as (
    select * from {{ref('stg_postgres__products')}}
),

staging_orders as (
    select * from {{ref('stg_postgres__orders')}}
),

int_transforms as (
    select 
        order_items.order_guid,
        order_items.product_guid,
        orders.user_guid,
        max(orders.order_placed_at_utc) over
            (partition by products.product_guid
            order by orders.order_placed_at_utc desc)
            as last_time_ordered_at_utc,
        products.product_name,
        products.product_price,
        order_items.quantity as product_quantity,
        products.product_price * product_quantity as product_subtotal
    from staging_order_items as order_items
    left join staging_products as products
        on order_items.product_guid = products.product_guid
    left join staging_orders as orders
        on order_items.order_guid = orders.order_guid
)

select * from int_transforms