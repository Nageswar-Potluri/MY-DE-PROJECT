WITH race_cte AS (
    SELECT
        track,
        race_number,
        race_date,
        race_name,
        distance_m,
        race_class,
        track_condition
    FROM workspace.silver.silver_harness_runners
)

SELECT *
FROM race_cte