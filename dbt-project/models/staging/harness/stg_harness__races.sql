{{ config(materialized='view') }}

select
    race_code,
    meeting_code,
    cast(race_date as date)      as race_date,
    track_name,
    cast(race_number as int)     as race_number,
    race_name,
    cast(total_runners as int)   as total_runners,
    class,
    race_type,
    track                        as track_slug,
    distance,
    cast(distance_metres as int) as distance_metres,
    source,
    condition,
    country,
    country_name,
    cast(prize_money as int)     as prize_money,
    _ingested_at,
    _bronze_loaded_at,
    _silver_loaded_at
from {{ source('harness_silver', 'harness_races') }}
