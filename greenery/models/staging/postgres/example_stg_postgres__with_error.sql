with
source as (
    select * from {{soup('doesnt_exist', 'some_model_name')}}
)

select * from source