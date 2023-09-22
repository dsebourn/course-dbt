with
staging_users as (
    select * from {{ref('stg_postgres__users')}}
),

staging_addresses as (
    select * from {{ref('stg_postgres__addresses')}}
),

prep_items_ordered as (
    select * from {{ref('int_postgres__order_items_details')}}
),

prep_order_history as (
    select * from {{ref('int_postgres__user_order_history')}}
),

int_transforms as (
    select
        users.user_guid,
        users.users_first_name,
        users.users_last_name,
        users.users_full_name,
        users.users_email,
        users.users_phone_number,
        users.user_account_created_at_utc,
        users.user_account_updated_at_utc,
        users.users_address_guid,
        addresses.address as users_address,
        addresses.zipcode as users_zipcode,
        addresses.state as users_state,
        addresses.country as users_country,
        addresses.full_address as users_full_address,
        count(distinct item_orders.order_guid) over
            (partition by users.user_guid)
            as count_of_user_lifetime_orders,
        count(distinct item_orders.product_guid) over
            (partition by users.user_guid)
            as count_of_user_lifetime_products,
        max(order_history.order_placed_at_utc) over
            (partition by users.user_guid)
            as last_order_placed_at_utc
    from staging_users as users
    left join staging_addresses as addresses
        on users.users_address_guid = addresses.address_guid
    left join prep_items_ordered as item_orders
        on users.user_guid = item_orders.user_guid
    left join prep_order_history as order_history
        on users.user_guid = order_history.user_guid
    qualify row_number() over
        (partition by users.user_guid
        order by users.user_account_updated_at_utc desc nulls last) = 1
)

select * from int_transforms