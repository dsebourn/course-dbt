with
user_conversions_daily as (
    select * from {{ ref('int_postgres__kpi_conversion_rates_users_daily') }}
),

product_conversions_daily as (
    select * from {{ ref('int_postgres__kpi_conversion_rates_products_daily') }}
),

prep_product_conversions_daily as (
    select
        order_session_began_on_date,
        sum(count_user_product_orders) as count_user_product_orders,
        sum(count_user_product_views) as count_user_product_views,
        avg(product_conversion_rate) as average_product_conversion_rate
    from 
        product_conversions_daily
    group by order_session_began_on_date
),

prep_conversions as (
    select
        user_conversions_daily.page_view_session_began_on_date,
        user_conversions_daily.count_user_order_sessions,
        user_conversions_daily.count_user_view_sessions,
        round(user_conversions_daily.daily_session_order_conversion_rate, 3)
            as daily_user_conversion_rate,
        prep_product_conversions_daily.count_user_product_orders,
        prep_product_conversions_daily.count_user_product_views,
        ifnull(round(prep_product_conversions_daily.average_product_conversion_rate, 3), 0)
            as daily_product_conversion_rate
    from user_conversions_daily
    left join prep_product_conversions_daily
        on prep_product_conversions_daily.order_session_began_on_date = user_conversions_daily.page_view_session_began_on_date
    order by page_view_session_began_on_date
)

select * from prep_conversions