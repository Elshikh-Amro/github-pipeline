SELECT DISTINCT
    repo_id,
    repo_name,
    owner,
    stars,
    language,
    description,
    url
FROM {{ ref('stg_repos') }}