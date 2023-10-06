<h3>Part 1. dbt Snapshots</h3>
1. Run dbt snapshot
    - Done

2. Which products changed?
    - Bamboo, Monstera, Philodendron, Pothos, String of pearls, ZZ Plant
    ```
with
updated_data as (
    select
        product_id,
        name,
        inventory as new_inventory,
        dbt_updated_at as product_inventory_updated_at_utc,
        count(dbt_valid_to) over
            (partition by product_id)
            as updated_indicator,
        case when updated_indicator > 0 then true else false
            end as product_updated
    from inventory_snap
    qualify row_number() over
        (partition by product_id
        order by dbt_updated_at desc) = 1
)

select * from updated_data where product_updated = true;
    ```

3. Which products had the most fluctuations in inventory? Did anything go out of stock?
    - Products String of pearls and ZZ Plant had the largest fluctuations in inventory, both with a 48 unit decrease
    ```
with
updated_data as (
    select
        product_id,
        name,
        inventory as new_inventory,
        case
            when inventory - lag(inventory, 1, 0) over
            (partition by product_id order by dbt_updated_at asc) = inventory
                then null
            else inventory - lag(inventory, 1, 0) over
            (partition by product_id order by dbt_updated_at asc)
        end as inventory_change,
        dbt_updated_at as product_inventory_updated_at_utc,
        count(dbt_valid_to) over
            (partition by product_id)
            as updated_indicator,
        case when updated_indicator > 0 then true else false
            end as product_updated
    from inventory_snap
)

select *
from updated_data
where updated_indicator > 0
order by inventory_change asc nulls last;
    ```
    
    - Looking at my model dim__products, there are two products with 0 inventory (Pothos and String of pearls), but this was not revealed by my snapshot.
    ```
select *
from dim__products
where current_product_inventory < 1;
    ```

<h3>Part 2. Modeling challenge</h3>
1. How are our users moving through the product funnel?
    - The typical customer behavior is that users will browse a few products before adding the ones they liked to their cart and purchasing them. There are some outliers to the behavior like customers removing products from their cart before purchasing, or customers purchasing the one product they knew they wanted.

2. Which steps in the funnel have largest drop off points?
    - Adding to the cart from page views has the largest drop off point, but not by much. For this last quarter, we lost 111 user sessions between product browsing and product selection. We then lost an additional 106 user sessions between product selection and product purchasing.
    ```
    select * from dim__quarterly_product_funnel;
    ```

BONUS: Visualization for Product Funnel: https://app.sigmacomputing.com/corise-dbt/workbook/Quarterly-Product-Funnel-Report-2uRywcw4uQZpH1yGd6KbkG?:explore=04a38e73-ea3f-4001-a021-2299c31e9ebb

<h3>Part 3: Reflection questions -- please answer 3A or 3B, or both!</h3>

<h5>3A. dbt next steps for you</h5>
1. if your organization is thinking about using dbt, how would you pitch the value of dbt/analytics engineering to a decision maker at your organization?
    - We already use dbt which was partially why I decided to take this course! If I was at an organization that was not using DBT, I would likely pitch dbt around the key point that it made managing our data incredibly easy and straightforward, we could develop new models with ease, test our data for any conditions, and even protect our data with posthooks and macros.

2. if your organization is using dbt, what are 1-2 things you might do differently / recommend to your organization based on learning from this course?
    - Honestly our organization is very established in their ways with dbt so I doubt I would be able to convince anyone to do anything differently. The thing I would likely be able to encourage more of is having our analysts write more robus yml files. Many of our ymls are baren and lacking in descriptions and tests, as this step of our development process isnt well enforced.

3. if you are thinking about moving to analytics engineering, what skills have you picked that give you the most confidence in pursuing this next step?
    - Having taken this course, I feel more confident than before that I can comfortably work within dbt and all the tools it brings.

<h5>3B. Setting up for production / scheduled dbt run of your project</h5>
1. how would you go about setting up a production/scheduled dbt run of your project in an ideal state? 
    - I would likely use Apache Airflow or Dagster. I would likely use Dagster to help ensure our data validation tests are happening on a regular and frequent basis, that our models are running timely with no errors (and to alert us if there is an issue), and that any data protection or obfuscation tasks take place without the need for a human to remember to do these. Dagster doesn’t need to run every model in their own separate task, so it saves on $ which teams greatly appreciate.