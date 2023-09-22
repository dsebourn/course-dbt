with

int_products as (
    select * from {{ref('int_postgres__products')}}
),

prep_final as (
    select
        product_name,
        product_price,
        current_product_inventory,
        lifetime_orders_of_product,
        lifetime_quantity_ordered,
        lifetime_distinct_users_ordered,
        days_since_ordered_last
    from int_products
)

select * from prep_final