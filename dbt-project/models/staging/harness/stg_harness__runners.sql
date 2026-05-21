{{ config(materialized='view') }}

select
    race_code,
    meeting_code,
    horse_name,
    horse_number,
    cast(barrier as int)         as barrier,
    jockey,
    trainer,
    age,
    sex,
    horse_country,
    gear,
    racing_colours,
    class_profile,
    recent_form,
    last_20,
    gear_changes,
    prize_money                  as career_prize_money,
    weight,
    sire,
    dam,
    _ingested_at,
    _bronze_loaded_at,
    _silver_loaded_at
from {{ source('harness_silver', 'harness_runners') }}
where not coalesce(is_scratched, false)
