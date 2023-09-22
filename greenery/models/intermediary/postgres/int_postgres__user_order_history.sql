with
int_order_details as (
    select * from {{ref('int_postgres__order_details')}}
),

int_transforms as (
    select
        order_guid,
        order_number,
        user_guid,
        users_full_name,
        order_placed_at_utc,
        list_of_products_ordered,
        order_piece_count,
        orders_total_amount,
        tracking_guid,
        order_status,
        case
            when order_status = 'preparing'
                then order_placed_at_utc
            when order_status = 'shipped'
                then estimated_delivery_at_utc
            when order_status = 'delivered'
                then actual_delivery_at_utc
            else null
        end as order_status_at_utc
    from int_order_details
)

select * from int_transforms