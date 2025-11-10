-- singular test using sql; 
-- similar to pytest, if this select returns any rows then there is/are failure(s).

with 

orders as (
    select * from {{ ref('raw_pos_order_header') }}
)


select
    order_id,
    sum(ORDER_AMOUNT) as total_amount
from orders
group by order_id
having total_amount < 0