{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['route_id', 'event_date'], 'unique': True},
      {'columns': ['event_date']},
    ]
  )
}}

with route_delays as (
    select * from {{ ref('int_route_delays') }}
),

final as (
    select
        md5(route_id || '|' || event_date::text) as id,
        route_id,
        event_date,
        total_events,
        on_time_count,
        minor_delay_count,
        moderate_delay_count,
        severe_delay_count,
        avg_delay_seconds,
        avg_delay_minutes,
        max_delay_seconds,
        median_delay_seconds,
        p95_delay_seconds,
        on_time_rate_pct,

        case
            when on_time_rate_pct >= 95 then 'A'
            when on_time_rate_pct >= 85 then 'B'
            when on_time_rate_pct >= 75 then 'C'
            when on_time_rate_pct >= 60 then 'D'
            else 'F'
        end as performance_grade,

        case
            when on_time_rate_pct < 75 then true
            else false
        end as slo_breached,

        current_timestamp as dbt_updated_at

    from route_delays
)

select * from final