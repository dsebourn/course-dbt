with
website_results_funnel as (
    select * from {{ ref('int_postgres__website_session_results') }}
),

quarterly_funnel_results as (
    select
        date_trunc('quarter', session_start_date) as product_funnel_quarter,
        count(array_of_products_viewed) as count_product_view_funnel,
        count(array_of_products_added_to_cart) as count_product_add_to_cart_funnel,
        count(array_of_products_ordered) as count_product_ordered_funnel,
        count_product_add_to_cart_funnel - count_product_view_funnel
            as product_add_to_cart_funnel_loss,
        count_product_ordered_funnel - count_product_add_to_cart_funnel
            as product_ordered_funnel_loss,
        product_add_to_cart_funnel_loss + product_ordered_funnel_loss
            as total_product_funnel_loss
    from website_results_funnel
    group by product_funnel_quarter
)

select *
from quarterly_funnel_results