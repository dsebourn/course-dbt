with
source as (
  select * from {{ source('postgres', 'users') }}
),

renamed_recast as (
    select
      user_id as user_guid,
      first_name as users_first_name,
      last_name as users_last_name,
      concat(users_first_name, ' ', users_last_name) as users_full_name,
      email as users_email,
      phone_number as users_phone_number,
      created_at as user_account_created_at_utc,
      updated_at as user_account_updated_at_utc,
      address_id as users_address_guid
    from source
)

select * from renamed_recast