{{ config(
    post_hook="{{ grant_select_on_model_to_roles(['TRANSFORMER_DEV', 'RESEARCH_ROLE', 'MARKETING_ROLE', 'REPORTING']) }}"
) }}

with
int_products as (
    select * from {{ref('int_postgres__products')}}
),

prep_final as (
    select
        product_guid,
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