SELECT
    c.sha,
    c.repo_name,
    c.author_name,
    r.repo_id,
    c.committed_at,
    c.committed_at::DATE AS date_day,
    c.message_length
FROM {{ ref('stg_commits') }} c
LEFT JOIN {{ ref('dim_repositories') }} r
    ON c.repo_name = r.repo_name