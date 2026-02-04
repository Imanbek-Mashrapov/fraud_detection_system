-- transactions insertion
INSERT INTO core.transactions (
    transaction_id,
    user_id,
    device_id,
    merchant_id,
    amount,
    currency,
    transaction_country,
    transaction_ts,
    is_fraud
)
SELECT
    transaction_id,
    user_id,
    device_id,
    merchant_id,
    amount,
    currency,
    transaction_country,
    transaction_ts,
    false AS is_fraud
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY ingestion_ts DESC
        ) AS rn
    FROM raw.transactions
) t
WHERE rn = 1
AND NOT EXISTS (
	SELECT 1
	FROM core.transactions ct
	WHERE ct.transaction_id = t.transaction_id
);
