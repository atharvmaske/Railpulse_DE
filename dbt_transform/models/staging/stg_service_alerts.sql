{{
  config(
    materialized = 'view'
  )
}}

with source as (
    select * from {{ source('railpulse_raw', 'service_alerts') }}
),

cleaned as (
    select
        id as alert_event_id,
        alert_id,
        cause,
        effect,
        header_text,
        route_id,
        valid_from,
        valid_until,
        created_at,

        case
            when valid_until is null then true
            when valid_until >= now() then true
            else false
        end as is_active,

        case
            when valid_from is not null and valid_until is not null
            then extract(epoch from (valid_until - valid_from)) / 3600
            else null
        end as duration_hours

    from source
)

select * from cleaned