{{ config(
    post_hook="{{ grant_select_on_model_to_roles(['TRANSFORMER_DEV', 'RESEARCH_ROLE', 'OPERATIONS_ROLE']) }}"
) }}

with
order_details as (
    select * from {{ref('int_postgres__order_details')}}
),

final as (
    select
        days_in_preparing_status,
        order_number,
        users_full_name, 
        delivery_full_address, 
        delivery_address, 
        delivery_zipcode, 
        delivery_state, 
        delivery_country, 
        order_placed_at_utc, 
        list_of_products_ordered, 
        order_piece_count
    from order_details
    where order_status = 'preparing'
)

select *
from final
order by days_in_preparing_status desc