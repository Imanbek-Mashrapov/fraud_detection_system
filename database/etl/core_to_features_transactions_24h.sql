INSERT INTO features.transaction_features_24h (
    transaction_id,
    tx_count_last_24h,
    avg_amount_last_24h,
    calculated_at
)
SELECT
    t.transaction_id,

    COUNT(t2.transaction_id) AS tx_count_last_24h,

    AVG(t2.amount) AS avg_amount_last_24h,

    NOW() AS calculated_at

FROM core.transactions t

LEFT JOIN core.transactions t2
  ON t2.user_id = t.user_id
 AND t2.transaction_ts < t.transaction_ts
 AND t2.transaction_ts >= t.transaction_ts - INTERVAL '24 hour'

WHERE NOT EXISTS (
    SELECT 1
    FROM features.transaction_features_24h f
    WHERE f.transaction_id = t.transaction_id
)

GROUP BY
    t.transaction_id;
