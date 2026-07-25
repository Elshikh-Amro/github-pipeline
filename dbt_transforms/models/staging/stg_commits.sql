SELECT
    sha,
    repo_name,
    author_name,
    LEFT(message, 200) AS message_preview,
    LENGTH(message) AS message_length,
    committed_at::TIMESTAMPTZ AS committed_at
FROM {{ source('raw', 'raw_commits') }}