with
source as (
    select * from {{ source('postgres', 'orders') }}
),

renamed_recast as (
    select
        order_id as order_guid,
        user_id as user_guid,
        promo_id,
        address_id as delivery_address_guid,
        created_at as order_placed_at_utc,
        order_cost as orders_product_cost,
        shipping_cost as orders_shipping_cost,
        order_total as orders_total_amount,
        tracking_id as tracking_guid,
        shipping_service as carrier_name,
        estimated_delivery_at as estimated_delivery_at_utc,
        delivered_at as actual_delivery_at_utc,
        status as order_status
    from source
)

select * from renamed_recast