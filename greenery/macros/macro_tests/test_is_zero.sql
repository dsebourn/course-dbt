{% macro test_is_zero(model, column_name) %}

with validation as (

    select
        {{ column_name }} as zero_field

    from {{ model }}

),

validation_errors as (

    select
        zero_field

    from validation
    -- if this is true, then there is a discount of 0%
    where zero_field = 0

)

select *
from validation_errors

{% endmacro %}