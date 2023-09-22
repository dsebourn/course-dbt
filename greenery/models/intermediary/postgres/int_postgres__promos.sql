with
staging as (
    select * from {{ref('stg_postgres__promos')}}
),

int_transforms as (
    select
        promo_id,
        lower(regexp_replace(promo_id, '\\-|\\ ', '_')) as promo_type,
        discount*0.01 as discount_percent,
        status
    from
        staging
)

select * from int_transforms