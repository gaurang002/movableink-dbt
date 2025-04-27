{{
    config(
        materialized='view'
    )
}}

with time_to_conversion as (
select  user_uuid,
		request_uuid,
		event_type,
		event_date_time,
		lag(case when event_type like '%open%' then event_date_time end) over (partition by user_uuid order by event_date_time) as prev_open_event_time,
		msecs,
		lag(case when event_type like '%open%' and object_type = 'campaign_pic' then msecs end) over (partition by user_uuid order by event_date_time) as prev_open_msec
from bronze.campaign_vw
), final as (
select 	user_uuid,
		request_uuid as conversion_request_uuid,
		msecs as convert_time,
		prev_open_msec as open_time
from time_to_conversion
where event_type = 'conversion'
and prev_open_msec is not null
order by event_date_time
)
select *
from final