INSERT INTO features.transaction_features_1h (
    transaction_id,
    tx_count_last_1h,
    avg_amount_last_1h,
    is_new_device,
    is_foreign_tx,
    calculated_at
)
SELECT
    t.transaction_id,

    COUNT(t2.transaction_id) AS tx_count_last_1h,

    AVG(t2.amount) AS avg_amount_last_1h,

    NOT EXISTS (
        SELECT 1
        FROM core.transactions t3
        WHERE t3.user_id = t.user_id
          AND t3.device_id = t.device_id
          AND t3.transaction_ts < t.transaction_ts
    ) AS is_new_device,

    (u.home_country <> t.transaction_country) AS is_foreign_tx,

    NOW() AS calculated_at

FROM core.transactions t

JOIN core.users u
  ON u.user_id = t.user_id

LEFT JOIN core.transactions t2
  ON t2.user_id = t.user_id
 AND t2.transaction_ts < t.transaction_ts
 AND t2.transaction_ts >= t.transaction_ts - INTERVAL '1 hour'

WHERE NOT EXISTS (
    SELECT 1
    FROM features.transaction_features_1h tf
    WHERE tf.transaction_id = t.transaction_id
)

GROUP BY
    t.transaction_id,
    t.user_id,
    t.device_id,
    u.home_country,
    t.transaction_country;
