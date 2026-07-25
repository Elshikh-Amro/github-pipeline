WITH date_spine AS (
    SELECT DISTINCT
        committed_at::DATE AS date_day
    FROM {{ ref('stg_commits') }}
    UNION
    SELECT DISTINCT
        created_at::DATE AS date_day
    FROM {{ ref('stg_issues') }}
)
SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DOW FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'Day') AS day_name
FROM date_spine