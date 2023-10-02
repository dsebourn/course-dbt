{{ config(
    post_hook="{{ grant_select_on_model_to_roles(['TRANSFORMER_DEV', 'RESEARCH_ROLE', 'MARKETING_ROLE', 'SALES_ROLE', 'PRODUCT_ROLE']) }}"
) }}

with
website_session_results as (
    select * from {{ref('int_postgres__website_session_missed_opportunities')}}
),

users as (
    select * from {{ref('int_postgres__users_details')}}
),

prep_final as (
    select
        results.session_guid,
        users.users_full_name,
        users.user_account_created_at_utc,
        users.count_of_user_lifetime_orders,
        users.count_of_user_lifetime_products,
        users.last_order_placed_at_utc,
        results.missed_opportunity_products
    from website_session_results as results
    left join users
        on results.user_guid = users.user_guid
)

select * from prep_final