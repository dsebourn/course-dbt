with

int_products as (
    select * from {{ref('int_postgres__products')}}
),

prep_final as (
    select
        product_name,
        product_price,
        current_product_inventory
    from int_products
)

select * from prep_final