-- users insertion
INSERT INTO core.users(
	user_id,
	registration_date,
	home_country,
	risk_segment,
	created_at
)
SELECT 
	user_id,
	registration_date::date,
	home_country,
	risk_segment,
	ingestion_ts AS created_at
FROM (
	SELECT 
		  *,
		  ROW_NUMBER() OVER(
		      PARTITION BY user_id
			  ORDER BY ingestion_ts DESC
	  ) AS rn
	FROM raw.users
) u
WHERE rn = 1
AND NOT EXISTS (
    SELECT 1
    FROM core.users cu
    WHERE cu.user_id = u.user_id
);