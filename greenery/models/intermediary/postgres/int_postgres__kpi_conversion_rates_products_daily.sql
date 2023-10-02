with
products_ordered as (
    select * from {{ref('int_postgres__order_items_details')}}
),

session_orders as (
    select * from {{ref('int_postgres__website_session_orders')}}
),

session_views as (
    select * from {{ref('int_postgres__website_session_views')}}
),

products as (
    select * from {{ref('int_postgres__products')}}
),

sessions_ordered_products as (
    select
        session_orders.session_guid,
        session_orders.order_guid,
        session_orders.order_session_began_on_date,
        products_ordered.product_guid,
        products_ordered.product_name,
        products_ordered.product_quantity
    from session_orders
    left join products_ordered
        on session_orders.order_guid = products_ordered.order_guid
),

sessions_viewed_products as (
    select
        session_views.session_guid,
        session_views.event_guid,
        session_views.page_view_session_began_on_date,
        products.product_guid,
        products.product_name
    from session_views
    left join products
        on session_views.product_guid = products.product_guid
),

prep_product_conversion_rates as (
    select
        distinct sessions_ordered_products.order_session_began_on_date,
        products.product_guid,
        products.product_name,
        count(distinct sessions_ordered_products.order_guid) over
            (partition by products.product_guid, order_session_began_on_date)
            as count_user_product_orders,
        count(distinct sessions_viewed_products.event_guid) over
            (partition by products.product_guid, order_session_began_on_date)
            as count_user_product_views,
        count_user_product_orders / count_user_product_views as product_conversion_rate
    from products
    left join sessions_ordered_products
        on products.product_guid = sessions_ordered_products.product_guid
    left join sessions_viewed_products
        on products.product_guid = sessions_viewed_products.product_guid
        and sessions_ordered_products.order_session_began_on_date = sessions_viewed_products.page_view_session_began_on_date
)

select * from prep_product_conversion_rates