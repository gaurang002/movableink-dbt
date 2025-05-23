{{
    config(
        materialized='view'
    )
}}

with usage_by_day as (
select 	event_date_time::date as day,
        company_id,
		campaign_pic_id,
		object_id,
		count(*) as amount
from bronze.campaign_vw
where object_type = 'rules_pic'
and event_type like '%open%'
group by event_date_time::date,
        company_id,
		campaign_pic_id,
		object_id
having count(*) > 1
), final as (
select *
from usage_by_day
order by day
)
select *
from final