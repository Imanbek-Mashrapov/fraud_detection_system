TRUNCATE TABLE features.user_behavior_features;

INSERT INTO features.user_behavior_features (
    user_id,
    tx_count_7d,
    avg_amount_7d,
    distinct_countries_7d,
    calculated_at
)
SELECT
    t.user_id,

    COUNT(t.transaction_id) AS tx_count_7d,

    AVG(t.amount) AS avg_amount_7d,

    COUNT(DISTINCT t.transaction_country) AS distinct_countries_7d,

    NOW() AS calculated_at

FROM core.transactions t

WHERE t.transaction_ts >= NOW() - INTERVAL '7 day'

GROUP BY
    t.user_id;
