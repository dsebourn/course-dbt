{{ config(
    post_hook="{{ grant_select_on_model_to_roles(['TRANSFORMER_DEV', 'RESEARCH_ROLE', 'MARKETING_ROLE', 'OPERATIONS_ROLE', 'SALES_ROLE', 'PRODUCT_ROLE']) }}"
) }}

with

int_products as (
    select * from {{ref('int_postgres__products')}}
),

prep_final as (
    select
        product_guid
        product_name,
        product_price,
        current_product_inventory
    from int_products
)

select * from prep_final