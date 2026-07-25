SELECT DISTINCT
    repo_id,
    repo_name,
    owner,
    language,
    description,
    url
FROM {{ ref('stg_repos') }}