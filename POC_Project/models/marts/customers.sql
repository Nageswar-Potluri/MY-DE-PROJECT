
-- -- SELECT table_name, table_type, last_altered
-- -- FROM workspace.information_schema.tables
-- -- WHERE table_schema = 'silver'


-- select * from workspace.silver.silver_harness_runners

-- select column_name, data_type 
-- from workspace.information_schema.columns 
--     where table_name = 'silver_harness_runners' 
--     and table_schema = 'silver' ; 



with race_cte as 
(
    select * from {{ref('race_cte')}}
),

runner_cte as 
(
    select * from {{ref('runners_cte')}}
)

SELECT
    r.track,
    r.race_number,
    r.race_date,
    r.race_name,
    r.distance_m,
    r.track_condition,
    ru.horse_name,
    ru.jockey,
    ru.trainer,
    ru.barrier,
    ru.weight_kg,
    ru.overall_win_pct
FROM race_cte r
JOIN runner_cte ru
    ON r.track = ru.track
   AND r.race_number = ru.race_number
   AND r.race_date = ru.race_date