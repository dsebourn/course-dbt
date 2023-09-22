with
order_details as (
    select * from {{ref('int_postgres__order_details')}}
),

final as (
    select
        order_number,
        users_full_name, 
        users_email, 
        users_phone_number, 
        users_full_address, 
        delivery_full_address, 
        delivery_address, 
        delivery_zipcode, 
        delivery_state, 
        delivery_country, 
        order_placed_at_utc, 
        list_of_products_ordered, 
        order_piece_count, 
        order_products_subtotal, 
        promo_type, 
        discount_percent, 
        orders_product_cost, 
        orders_shipping_cost, 
        orders_total_amount, 
        carrier_name, 
        estimated_delivery_at_utc, 
        actual_delivery_at_utc, 
        order_status
    from order_details
)

select * from final