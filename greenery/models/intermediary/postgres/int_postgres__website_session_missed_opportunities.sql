with
events as (
    select * from {{ref('stg_postgres__events')}}
),

results as (
    select * from {{ref('int_postgres__website_session_results')}}
),

missed_opportunities as (
    select
        events.session_guid,
        events.user_guid,
        results.array_of_missed_opportunity_products as missed_opportunity_products
    from events
    left join results
        on events.session_guid = results.session_guid
    where array_of_missed_opportunity_products is not null
    qualify row_number() over
        (partition by events.session_guid
        order by events.session_guid) = 1
)

select * from missed_opportunities