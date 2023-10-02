1. What is our overall conversion rate?

```
select avg(daily_user_conversion_rate) from fct__kpi_conversion_rates
;
```

A: 82% over the three days of purchases

------------------------

2. What is our conversion rate by product?

```
select avg(daily_product_conversion_rate) from fct__kpi_conversion_rates
;
```

A: 66% over the three days of purchases
