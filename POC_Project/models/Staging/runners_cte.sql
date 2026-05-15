WITH runner_cte AS (
    SELECT
        race_date,
        track,
        race_number,
        horse_name,
        jockey,
        trainer,
        barrier,
        weight_kg,
        overall_starts,
        overall_wins,
        overall_win_pct
    FROM workspace.silver.silver_harness_runners
    WHERE scratched = false
)

SELECT *
FROM runner_cte