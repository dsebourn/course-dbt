with
source as (
    select * from {{ source('postgres', 'products') }}
),

renamed_recast as (
    select
        product_id as product_guid,
        name as product_name,
        price as product_price,
        inventory as current_product_inventory
    from source
)

select * from renamed_recast