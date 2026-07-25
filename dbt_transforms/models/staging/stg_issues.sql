SELECT
    id AS issue_id,
    repo_name,
    title,
    state,
    created_at::TIMESTAMPTZ AS created_at
FROM {{ source('raw', 'raw_issues') }}