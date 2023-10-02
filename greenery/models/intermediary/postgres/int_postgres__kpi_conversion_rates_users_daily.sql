with
page_views as (
    select * from {{ ref('int_postgres__website_session_views') }}
),

page_orders as (
    select * from {{ ref('int_postgres__website_session_orders') }}
),

prep_order_conversions as (
    select
        page_views.page_view_session_began_on_date,
        count(distinct page_orders.session_guid) over
            (partition by page_views.page_view_session_began_on_date)
            as count_user_order_sessions,
        count(distinct page_views.session_guid) over
            (partition by page_views.page_view_session_began_on_date)
            as count_user_view_sessions,
        count_user_order_sessions / count_user_view_sessions as daily_session_order_conversion_rate
    from page_views
    left join page_orders
        on page_views.page_view_session_began_on_date = page_orders.order_session_began_on_date
    qualify row_number() over
        (partition by page_views.page_view_session_began_on_date
        order by page_views.page_view_session_began_on_date asc) = 1
)

select * from prep_order_conversions