create table if not exists bronze.campaign (
    request_uuid text,
    object_type text,
    msecs BIGINT,
    payload jsonb,
    unique (request_uuid, object_type, msecs)
);