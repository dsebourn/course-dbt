{{ config(
    post_hook="{{ grant_select_on_model_to_roles(['TRANSFORMER_DEV', 'RESEARCH_ROLE', 'MARKETING_ROLE', 'REPORTING']) }}"
) }}

with
kpi_all_conversion_rates as (
    select * from {{ ref('int_postgres__kpi_all_conversion_rates')}}
)

select * from kpi_all_conversion_rates