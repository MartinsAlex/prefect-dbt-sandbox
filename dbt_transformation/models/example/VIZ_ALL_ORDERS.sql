
{{ config(materialized='table', full_refresh=true) }}


SELECT 
    orders.*,
    customers.email
from {{ source('public', 'orders') }} orders
left join {{ source('public', 'customers') }} customers ON (orders.customer_id = customers.customer_id)
