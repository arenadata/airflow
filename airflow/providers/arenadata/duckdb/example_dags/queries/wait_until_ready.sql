SELECT count(*) AS ready
FROM {{ params.table }}
WHERE id >= {{ params.min_id }}
