{{
    config(
        materialized='view'
    )
}}

with request_duration_by_device_type as (
select 	device_category ,
		avg(duration) as avg_duration,
		stddev_pop(duration) as stddev_duration,
		count(*) as count
from bronze.campaign_vw
where object_type = 'live_pic'
group by device_category
), final as (
select *
from request_duration_by_device_type
)
select *
from final