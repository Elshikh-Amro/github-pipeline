SELECT
    i.issue_id,
    i.repo_name,
    r.repo_id,
    i.state,
    i.created_at,
    i.closed_at,
    i.is_closed,
    i.days_to_close,
    i.created_at::DATE AS date_day,
    d.date_key
FROM {{ ref('stg_issues') }} i
LEFT JOIN {{ ref('dim_repositories') }} r
    ON i.repo_name = r.repo_name
LEFT JOIN {{ ref('dim_dates') }} d
    ON i.created_at::DATE = d.date_day