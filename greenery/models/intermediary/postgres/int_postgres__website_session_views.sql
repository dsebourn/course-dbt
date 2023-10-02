with

view_sessions as (
    select
        event_guid,
        session_guid,
        product_guid,
        session_began_at_utc as page_views_began_at_utc,
        page_views_began_at_utc::date as page_view_session_began_on_date
    from {{ref('stg_postgres__events')}}
    where event_type = 'page_view'
)

select * from view_sessions