{{ 
    config(
        materialized='table',
        post_hook=[
        "INSERT INTO audit_logs (model_name, start_time, end_time, record_count, status)
        VALUES ('{{ this.name }}', CAST('{{ run_started_at }}' AS TIMESTAMP_NTZ), CURRENT_TIMESTAMP, (SELECT COUNT(*) FROM {{ this }}), 'success')"
    ]
    ) 
}}

with item_categories as (
    select distinct item_category
    from {{ source('tb_101', 'MENU') }}
)

select

    item_category,
    
    {% for item_cat in item_categories %}
        sum(case when item_category = '{{ item_cat }}' then COST_OF_GOODS_USD else 0 end) as {{ item_cat }}_cogs {% if not loop.last %},{% endif %}
    {% endfor %}
    
from {{ source('tb_101', 'MENU') }}
group by 1