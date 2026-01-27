-- Green Comtrade Bench Leaderboard Query
-- Shows only the LATEST run per agent, with percentage-based scoring
--
-- Output columns:
--   Agent: agent name/id
--   Total Score: percentage (0-100), calculated as 100 * sum(scores) / max_possible
--   Tasks: number of tasks in this run (typically 7)
--   Pass: "PASS" if Total Score >= 90.0, else "FAIL"
--   Latest Result: timestamp of the most recent run
--
-- This query prevents accumulation across multiple runs by:
-- 1. Grouping results by run (using filename as run identifier)
-- 2. Taking only the latest run per agent (using row_number)

WITH
-- Step 1: Flatten all JSON files into individual task results with run info
flat AS (
    SELECT
        filename,
        -- Extract agent_id from participants (first key)
        json_extract_string(participants, '$.' || (SELECT key FROM (SELECT unnest(json_keys(participants)) AS key) LIMIT 1)) AS agent_id,
        -- Extract agent_name from participants (first key name)
        (SELECT key FROM (SELECT unnest(json_keys(participants)) AS key) LIMIT 1) AS agent_name,
        -- Extract timestamp from filename (format: name-YYYYMMDD-HHMMSS.json)
        regexp_extract(filename, '(\d{8}-\d{6})\.json$', 1) AS run_timestamp,
        -- Unnest the results array (it's array of arrays, so we need to flatten twice)
        unnest(results) AS task_results_array
    FROM read_json_auto('results/*.json', filename=true)
),

-- Step 2: Further flatten to individual task results
tasks AS (
    SELECT
        filename,
        agent_id,
        agent_name,
        run_timestamp,
        unnest(task_results_array) AS task_result
    FROM flat
),

-- Step 3: Extract task scores
task_scores AS (
    SELECT
        filename,
        agent_id,
        agent_name,
        run_timestamp,
        json_extract_string(task_result, '$.task_id') AS task_id,
        CAST(json_extract(task_result, '$.score_total') AS DOUBLE) AS score_total
    FROM tasks
),

-- Step 4: Aggregate per run (per file)
per_run AS (
    SELECT
        filename,
        agent_id,
        agent_name,
        run_timestamp,
        SUM(score_total) AS total_score,
        COUNT(*) AS task_count,
        COUNT(*) * 100.0 AS max_possible  -- Each task is worth 100 points
    FROM task_scores
    GROUP BY filename, agent_id, agent_name, run_timestamp
),

-- Step 5: Rank runs per agent (latest first)
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY run_timestamp DESC) AS rn
    FROM per_run
),

-- Step 6: Take only the latest run per agent
latest AS (
    SELECT * FROM ranked WHERE rn = 1
)

-- Final output
SELECT
    agent_name AS "Agent",
    ROUND(100.0 * total_score / max_possible, 1) AS "Total Score",
    task_count AS "Tasks",
    CASE 
        WHEN (100.0 * total_score / max_possible) >= 90.0 THEN 'PASS'
        ELSE 'FAIL'
    END AS "Pass",
    run_timestamp AS "Latest Result"
FROM latest
ORDER BY "Total Score" DESC;
