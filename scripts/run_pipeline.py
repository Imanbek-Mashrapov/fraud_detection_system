import subprocess

print("STEP 1: generate raw data")
subprocess.run(["python", "-m", "data_generator.run"], check=True)

print("STEP 2: raw → core")
subprocess.run(["psql", "-f", "database/etl/raw_to_core_users.sql"], check=True)
subprocess.run(["psql", "-f", "database/etl/raw_to_core_transactions.sql"], check=True)

print("STEP 3: core → features")
subprocess.run(["psql", "-f", "database/etl/core_to_transaction_feature_1h.sql"], check=True)
subprocess.run(["psql", "-f", "database/etl/core_to_transaction_feature_24h.sql"], check=True)
subprocess.run(["psql", "-f", "database/etl/core_to_user_behavior_feature.sql"], check=True)

print("PIPELINE FINISHED")
