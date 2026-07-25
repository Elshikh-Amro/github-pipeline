SELECT
    id AS repo_id,
    name AS repo_name,
    owner,
    stars,
    forks,
    language,
    COALESCE(description, 'No description') AS description,
    url,
    fetched_at::DATE AS fetched_date
FROM {{ source('raw', 'raw_repos') }}