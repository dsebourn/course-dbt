with

int_users as (
    select * from {{ref('int_postgres__users_details')}}
),

prep_final as (
    select
        users_first_name, 
        users_last_name, 
        users_full_name, 
        users_email, 
        users_phone_number, 
        user_account_created_at_utc, 
        user_account_updated_at_utc, 
        users_full_address, 
        users_address, 
        users_zipcode, 
        users_state, 
        users_country, 
        count_of_user_lifetime_orders, 
        count_of_user_lifetime_products, 
        last_order_placed_at_utc
    from int_users
)

select * from prep_final