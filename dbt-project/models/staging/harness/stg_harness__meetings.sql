{{ config(materialized='view') }}

select
    meeting_id,
    track_slug,
    cast(meeting_date as date)  as meeting_date,
    racing_code,
    state,
    country,
    country_name,
    meeting_name,
    cast(number_of_races as int) as number_of_races,
    track_condition,
    weather,
    _ingested_at,
    _bronze_loaded_at,
    _silver_loaded_at
from {{ source('harness_silver', 'harness_meetings') }}
