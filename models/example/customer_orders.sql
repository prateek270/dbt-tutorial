-- models/customer_orders.sql

{{ config(
    materialized='view'
) }}

with orders as (
    select
        ordernumber,
        orderdate,
        timeinday,
        requiredate,
        shipdate,
        status,
        customernumber,
        comment
    from {{ source('dbf', 'orders') }}  -- Reference the source table
),

customer_orders as (
    select
        customernumber,
        count(ordernumber) as number_of_orders,
        min(orderdate) as first_order_date,
        max(orderdate) as most_recent_order_date
    from orders
    group by customernumber
)

select
    customernumber,
    number_of_orders,
    first_order_date,
    most_recent_order_date
from customer_orders
