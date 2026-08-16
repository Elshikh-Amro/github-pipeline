SELECT
    id AS issue_id,
    repo_name,
    title,
    state,
    created_at::TIMESTAMPTZ AS created_at,
    closed_at::TIMESTAMPTZ AS closed_at,
    closed_at IS NOT NULL AS is_closed,
    EXTRACT(EPOCH FROM (closed_at - created_at)) / 86400 AS days_to_close
FROM {{ source('raw', 'raw_issues') }}
