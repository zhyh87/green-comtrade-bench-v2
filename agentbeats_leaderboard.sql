-- Green Comtrade Bench Leaderboard Query
-- Shows only the LATEST run per agent, with percentage-based scoring (AVG)
--
-- Output columns:
--   Agent: agent UUID
--   Total Score: AVG(score_total) as percentage 0-100
--   Tasks: number of tasks in this run (typically 7)
--   Pass: "PASS" if Total Score >= 90.0, else "FAIL"
--   Latest Result: timestamp extracted from filename
--
-- Pass threshold: 90%

-- =============================================================================
-- VERSION 1: For AgentBeats platform (uses 'results' table)
-- =============================================================================
-- Paste this JSON into AgentBeats Leaderboard Queries:
/*
[
  {
    "name": "Overall Performance",
    "query": "WITH per_run AS (SELECT rs.participants.\"purple-comtrade-baseline-v2\" AS agent_id, regexp_extract(rs.filename, '(\\d{8}-\\d{6})', 1) AS run_time, ROUND(AVG(r.score_total), 1) AS total_score, COUNT(*) AS tasks FROM results AS rs CROSS JOIN UNNEST(rs.results[1]) AS t(r) WHERE rs.results IS NOT NULL GROUP BY rs.participants.\"purple-comtrade-baseline-v2\", rs.filename), latest AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY run_time DESC) AS rn FROM per_run) SELECT agent_id AS \"Agent\", total_score AS \"Total Score\", tasks AS \"Tasks\", CASE WHEN total_score >= 90.0 THEN 'PASS' ELSE 'FAIL' END AS \"Pass\", run_time AS \"Latest Result\" FROM latest WHERE rn = 1 ORDER BY total_score DESC"
  }
]
*/

-- =============================================================================
-- VERSION 2: For local DuckDB testing (uses read_json_auto)
-- =============================================================================
WITH per_run AS (
    SELECT 
        rs.participants."purple-comtrade-baseline-v2" AS agent_id,
        regexp_extract(rs.filename, '(\d{8}-\d{6})', 1) AS run_time,
        ROUND(AVG(r.score_total), 1) AS total_score,
        COUNT(*) AS tasks
    FROM read_json_auto('results/*.json', filename=true) AS rs
    CROSS JOIN UNNEST(rs.results[1]) AS t(r)
    WHERE rs.results IS NOT NULL
    GROUP BY rs.participants."purple-comtrade-baseline-v2", rs.filename
),
latest AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY run_time DESC) AS rn
    FROM per_run
)
SELECT 
    agent_id AS "Agent",
    total_score AS "Total Score",
    tasks AS "Tasks",
    CASE WHEN total_score >= 90.0 THEN 'PASS' ELSE 'FAIL' END AS "Pass",
    run_time AS "Latest Result"
FROM latest
WHERE rn = 1
ORDER BY total_score DESC;
