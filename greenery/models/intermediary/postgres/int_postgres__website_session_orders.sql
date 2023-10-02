with

order_sessions as (
    select
        session_guid,
        order_guid,
        session_began_at_utc as order_session_began_at_utc,
        order_session_began_at_utc::date as order_session_began_on_date
    from {{ref('stg_postgres__events')}}
    where event_type = 'checkout'
)

select * from order_sessions