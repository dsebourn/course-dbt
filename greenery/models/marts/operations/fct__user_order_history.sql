{{ config(
    post_hook="{{ grant_select_on_model_to_roles(['TRANSFORMER_DEV', 'RESEARCH_ROLE', 'OPERATIONS_ROLE']) }}"
) }}

with
order_history as (
    select * from {{ref('int_postgres__user_order_history')}}
),

final as (
    select
        order_number, 
        users_full_name, 
        order_placed_at_utc, 
        list_of_products_ordered, 
        order_piece_count, 
        orders_total_amount, 
        order_status, 
        order_status_at_utc
    from order_history
)

select *
from final
order by order_placed_at_utc desc