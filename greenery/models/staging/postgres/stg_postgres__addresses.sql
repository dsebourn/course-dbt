with
source as (
    select * from {{ source('postgres', 'addresses')}}
),

renamed_recast as (
    select
        address_id as address_guid,
        address,
        zipcode,
        state,
        country,
        concat(address, ', ', zipcode, ', ', state, ', ', country) as full_address
    from source
)

select * from renamed_recast