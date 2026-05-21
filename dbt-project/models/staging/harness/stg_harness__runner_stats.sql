{{ config(materialized='view') }}

select
    race_id,
    horse_name,
    horse_number,
    stat_group,
    cast(starts  as int)    as starts,
    cast(wins    as int)    as wins,
    cast(places  as int)    as places,
    cast(seconds as int)    as seconds,
    cast(thirds  as int)    as thirds,
    cast(win_pct   as double) as win_pct,
    cast(place_pct as double) as place_pct
from {{ source('harness_silver', 'harness_runner_stats') }}
