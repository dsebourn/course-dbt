with
staging_orders as (
    select * from {{ref('stg_postgres__orders')}}
),

int_order_numbers as (
    select * from {{ref('int_postgres__order_number_log')}}
),

int_order_item_details as (
    select
        order_guid,
        listagg(product_name, ', ') over
            (partition by order_guid)
            as list_of_products_ordered,
        sum(product_quantity) over
            (partition by order_guid)
            as order_piece_count,
        sum(product_subtotal) over
            (partition by order_guid)
            as order_products_subtotal
    from {{ref('int_postgres__order_items_details')}}
    qualify row_number() over
        (partition by order_guid
        order by order_guid) = 1

),

int_users_details as (
    select * from {{ref('stg_postgres__users')}}
),

staging_user_addresses as (
    select * from {{ref('stg_postgres__addresses')}}
),

int_delivery_addresses as (
    select * from {{ref('int_postgres__delivery_addresses')}}
),

int_promos as (
    select * from {{ref('int_postgres__promos')}}
),

int_transforms as (
    select
        orders.order_guid,
        order_number.order_number,
        customers.user_guid,
        customers.users_full_name,
        customers.users_email,
        customers.users_phone_number,
        user_addr.full_address as users_full_address,
        delivery_addr.delivery_address_guid,
        delivery_addr.delivery_full_address,
        delivery_addr.delivery_address,
        delivery_addr.delivery_zipcode,
        delivery_addr.delivery_state,
        delivery_addr.delivery_country,
        orders.order_placed_at_utc,
        items.list_of_products_ordered,
        items.order_piece_count,
        items.order_products_subtotal,
        promos.promo_type,
        promos.discount_percent,
        orders.orders_product_cost,
        orders.orders_shipping_cost,
        orders.orders_total_amount,
        orders.tracking_guid,
        orders.carrier_name,
        orders.estimated_delivery_at_utc,
        orders.actual_delivery_at_utc,
        orders.order_status,
        case
            when order_status = 'preparing'
                then datediff(days, orders.order_placed_at_utc::date, current_date())
            else null
        end as days_in_preparing_status
    from staging_orders as orders
    left join int_order_numbers as order_number
        on orders.order_guid = order_number.order_guid
    left join int_order_item_details as items
        on orders.order_guid = items.order_guid
    left join int_users_details as customers
        on orders.user_guid = customers.user_guid
    left join staging_user_addresses as user_addr
        on customers.users_address_guid = user_addr.address_guid
    left join int_delivery_addresses as delivery_addr
        on orders.delivery_address_guid = delivery_addr.delivery_address_guid
    left join int_promos as promos
        on orders.promo_id = promos.promo_id
)

select * from int_transforms