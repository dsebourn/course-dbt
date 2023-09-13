with
source as (
    select * from {{ source('postgres', 'products') }}
),

renamed_recast as (
    select
        product_id as product_guid,
        name,
        price,
        inventory
    from source
)

select * from renamed_recast