{{
    config(
        materialized='view'
    )
}}


with most_valuable_time as (
select 	company_id,
		extract(hour from event_date_time) as hour,
		count(*) as count,
		sum(amount) as amount
from bronze.campaign_vw
where event_type = 'conversion'
group by company_id,
		extract(hour from event_date_time)
), final as (
select 	*,
		amount / count as average
from most_valuable_time
order by company_id, hour
)
select *
from final