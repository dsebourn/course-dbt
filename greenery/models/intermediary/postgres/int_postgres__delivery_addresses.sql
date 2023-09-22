with
staging_orders as (
    select * from {{ref('stg_postgres__orders')}}
),

staging_addresses as (
    select * from {{ref('stg_postgres__addresses')}}
),

int_transforms as (
    select
        orders.delivery_address_guid,
        addresses.address as delivery_address,
        addresses.zipcode as delivery_zipcode,
        addresses.state as delivery_state,
        addresses.country as delivery_country,
        addresses.full_address as delivery_full_address
    from staging_orders as orders
    left join staging_addresses as addresses
        on orders.delivery_address_guid = addresses.address_guid
    qualify row_number() over
        (partition by orders.delivery_address_guid
        order by orders.delivery_address_guid) = 1
)

select * from int_transforms