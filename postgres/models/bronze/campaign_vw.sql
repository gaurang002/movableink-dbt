{{
    config(
        materialized='view'
    )
}}

select 	request_uuid,
	   	object_type,
	   	msecs,
	   	to_timestamp(msecs / 1000) as event_date_time,
	   	coalesce(payload->>'amount', '0')::double precision as amount,
	   	coalesce(payload->>'campaign_pic_id', '0')::bigint as campaign_pic_id,
		coalesce(payload->>'choice_override', '0')::bigint as choice_override,
	   	payload->>'client_name' as client_name,
		payload->>'client_type' as client_type,
	   	coalesce(payload->>'company_id', '0')::bigint as company_id,
		payload->>'dc' as dc,
	   	payload->>'device_category' as device_category,
		payload->>'device_platform' as device_platform,
	   	payload->>'device_type' as device_type,
		coalesce(payload->>'disabled', 'false')::boolean as disabled,
	   	coalesce(payload->>'duration', '0')::bigint as duration,
		payload->>'email_uuid' as email_uuid,
	   	payload->>'event_type' as event_type,
		coalesce(payload->>'image_id', '0')::bigint as image_id,
		payload->>'image_type' as image_type,
		coalesce(payload->>'object_id', '0')::bigint as object_id,
		payload->>'optimization_mode' as optimization_mode,
		coalesce(payload->>'optimization_num_positions', '0')::bigint as optimization_num_positions,
		coalesce(payload->>'optimization_position', '0')::bigint as optimization_position,
		coalesce(payload->>'optimization_probability', '0')::double precision as optimization_probability,
		coalesce(payload->>'schema_version', '0')::bigint as schema_version,
		payload->>'user_uuid' as user_uuid,
		payload->>'weather_conditions' as weather_conditions,
		payload->>'weather_icao' as weather_icao,
		coalesce(payload->>'weather_temperature', '0')::bigint as weather_temperature,
		coalesce(payload->>'weather_temperature_f', '0')::bigint as weather_temperature_f
from   bronze.campaign
where <date>