from database.connection import get_connection
import pandas as pd
from sklearn.metrics import roc_auc_score, average_precision_score, precision_recall_curve
import lightgbm as lgb
import joblib
from datetime import datetime
import json
import os

MODEL_NAME = "lightgbm_fraud"
MODEL_VERSION = f"lgb_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
MODEL_DIR = "ml/models"


def load_training_dataset():
    conn = get_connection()
    query = """
        SELECT *
        FROM features.training_dataset
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def preprocess(df):

    df = df.sort_values("transaction_ts")

    n = len(df)
    train_end = int(n * 0.7)
    val_end = int(n * 0.8)

    train_df = df.iloc[:train_end]
    val_df = df.iloc[train_end:val_end]
    test_df = df.iloc[val_end:]

    drop_cols = ["transaction_id", "is_fraud", "transaction_ts"]

    y_train = train_df["is_fraud"].astype(int)
    X_train = train_df.drop(columns=drop_cols)

    y_val = val_df["is_fraud"].astype(int)
    X_val = val_df.drop(columns=drop_cols)

    y_test = test_df["is_fraud"].astype(int)
    X_test = test_df.drop(columns=drop_cols)

    # converting bool to int
    bool_cols = X_train.select_dtypes(include="bool").columns
    X_train[bool_cols] = X_train[bool_cols].astype(int)
    X_val[bool_cols] = X_val[bool_cols].astype(int)
    X_test[bool_cols] = X_test[bool_cols].astype(int)

    return X_train, X_val, X_test, y_train, y_val, y_test


def train_model(X_train, y_train, X_val, y_val):

    model = lgb.LGBMClassifier(
        objective="binary",
        class_weight="balanced",
        n_estimators=2000,
        learning_rate=0.05,
        num_leaves=64,
        random_state=42
    )

    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        eval_metric="auc",
        early_stopping_rounds=100,
        verbose=100
    )

    return model


def select_threshold(model, X_val, y_val, min_precision=0.9):

    probs = model.predict_proba(X_val)[:, 1]
    precision, recall, thresholds = precision_recall_curve(y_val, probs)

    best_threshold = 0.5
    best_recall = 0

    for p, r, t in zip(precision, recall, thresholds):
        if p >= min_precision and r > best_recall:
            best_threshold = t
            best_recall = r

    print(f"Selected threshold: {best_threshold}")
    return best_threshold


def evaluate(model, X_test, y_test):

    probs = model.predict_proba(X_test)[:, 1]

    roc = roc_auc_score(y_test, probs)
    pr = average_precision_score(y_test, probs)

    print(f"Test ROC-AUC: {roc}")
    print(f"Test PR-AUC: {pr}")

    return roc, pr


def log_model_and_metrics(roc, pr):

    conn = get_connection()
    cur = conn.cursor()

    # deactivating previous models
    cur.execute("""
        UPDATE meta.model_registry
        SET is_active = FALSE
        WHERE model_name = %s
    """, (MODEL_NAME,))

    # inserting new model
    cur.execute("""
        INSERT INTO meta.model_registry
        (model_name, model_version, description, trained_at, is_active)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        MODEL_NAME,
        MODEL_VERSION,
        "LightGBM fraud model with temporal split",
        datetime.utcnow(),
        True
    ))

    # inserting metrics
    cur.execute("""
        INSERT INTO mart.model_metrics_daily
        (metric_date, model_version, auc, precision, recall)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        datetime.utcnow().date(),
        MODEL_VERSION,
        roc,
        None,  
        None
    ))

    conn.commit()
    conn.close()


def main():

    if not os.path.exists(MODEL_DIR):
        os.makedirs(MODEL_DIR)

    df = load_training_dataset()

    X_train, X_val, X_test, y_train, y_val, y_test = preprocess(df)

    model = train_model(X_train, y_train, X_val, y_val)

    threshold = select_threshold(model, X_val, y_val)

    roc, pr = evaluate(model, X_test, y_test)

    # saving model
    model_path = f"{MODEL_DIR}/{MODEL_VERSION}.pkl"
    joblib.dump(model, model_path)

    # save threshold
    with open(f"{MODEL_DIR}/{MODEL_VERSION}_threshold.json", "w") as f:
        json.dump({"threshold": float(threshold)}, f)

    log_model_and_metrics(roc, pr)

    print(f"Model saved: {model_path}")


if __name__ == "__main__":
    main()
