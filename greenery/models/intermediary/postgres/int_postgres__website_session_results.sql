with
web_events as (
    select *,
        min(created_at_utc) over
            (partition by session_guid)
            as session_start_at_utc
    from {{ref('stg_postgres__events')}}
),

order_items as (
    select * from {{ref('stg_postgres__order_items')}}
),

prep_web_views as (
    select
        events.session_guid,
        -- events.product_guid,
        events.event_type as viewing_event,
        array_agg(distinct items.product_guid) over
            (partition by events.session_guid) --, events.product_guid)
            as array_of_products_viewed
    from web_events as events
    left join order_items as items
        on events.product_guid = items.product_guid
    where event_type = 'page_view'
    qualify row_number() over
        (partition by events.session_guid
        order by events.session_guid) = 1
),

prep_web_cart_adds as (
    select
        events.session_guid,
        -- events.product_guid,
        events.event_type as cart_event,
        array_agg(distinct items.product_guid) over
            (partition by events.session_guid) --, events.product_guid)
            as array_of_products_added_to_cart
    from web_events as events
    left join order_items as items
        on events.product_guid = items.product_guid
    where event_type = 'add_to_cart'
    qualify row_number() over
        (partition by events.session_guid
        order by events.session_guid) = 1
),

prep_web_ordered_items as (
    select
        events.session_guid,
        events.order_guid,
        events.event_type as checkout_event,
        array_agg(distinct items.product_guid) over
            (partition by events.session_guid, events.order_guid)
            as array_of_products_ordered
    from web_events as events
    left join order_items as items
        on events.order_guid = items.order_guid
    where event_type = 'checkout'
    qualify row_number() over
        (partition by events.order_guid
        order by events.order_guid) = 1
),

prep_web_order_shipped as (
    select
        events.session_guid,
        events.order_guid,
        events.event_type as shipping_event,
        array_agg(distinct items.product_guid) over
            (partition by events.session_guid, events.order_guid)
            as array_of_products_shipped
    from web_events as events
    left join order_items as items
        on events.order_guid = items.order_guid
    where event_type = 'package_shipped'
    qualify row_number() over
        (partition by events.order_guid
        order by events.order_guid) = 1
),


prep_final_web_views as (
    select
        session_guid,
        -- product_guid,
        viewing_event,
        array_sort(array_of_products_viewed) as array_of_products_viewed
    from prep_web_views
),

prep_final_web_cart_adds as (
    select
        session_guid,
        -- product_guid,
        cart_event,
        array_sort(array_of_products_added_to_cart) as array_of_products_added_to_cart
    from prep_web_cart_adds
),

prep_final_web_orders as (
    select
        session_guid,
        order_guid,
        checkout_event,
        array_sort(array_of_products_ordered) as array_of_products_ordered
    from prep_web_ordered_items
),

prep_final_web_orders_shipped as (
    select
        session_guid,
        order_guid,
        shipping_event,
        array_sort(array_of_products_shipped) as array_of_products_shipped
    from prep_web_order_shipped
),

prep_final as (
    select
        distinct events.session_guid,
        events.session_start_at_utc::date as session_start_date,
        pfwv.array_of_products_viewed,
        pfwca.array_of_products_added_to_cart,
        pfwo.array_of_products_ordered,
        pfwos.array_of_products_shipped,
        case
            when array_size(pfwca.array_of_products_added_to_cart) is not null
            and array_size(pfwo.array_of_products_ordered) is null
                then 'cart_abandoned'
            when array_size(pfwv.array_of_products_viewed) > array_size(pfwca.array_of_products_added_to_cart)
            and array_size(pfwca.array_of_products_added_to_cart) is not null
                then 'missed_opportunity'
            when array_size(pfwca.array_of_products_added_to_cart) > array_size(pfwo.array_of_products_ordered)
            and array_size(pfwca.array_of_products_added_to_cart) is not null
                then 'changed_mind'
            when array_size(pfwo.array_of_products_ordered) > 0
            and array_size(pfwos.array_of_products_shipped) is null
                then 'order_not_shipped'
            when array_size(pfwca.array_of_products_added_to_cart) is null
                then 'browsing'
            else 'direct_purchase'
        end as web_session_notes,
        case
            when web_session_notes = 'missed_opportunity'
                then array_except(pfwv.array_of_products_viewed,
                    array_intersection(pfwv.array_of_products_viewed, pfwca.array_of_products_added_to_cart)
            ) else null
        end as array_of_missed_opportunity_products
    from web_events as events
    left join prep_final_web_views as pfwv
        on events.session_guid = pfwv.session_guid
    left join prep_final_web_cart_adds as pfwca
        on events.session_guid = pfwca.session_guid
    left join prep_final_web_orders as pfwo
        on events.session_guid = pfwo.session_guid
    left join prep_final_web_orders_shipped as pfwos
        on events.session_guid = pfwos.session_guid
)

select *
from prep_final