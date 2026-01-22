from datetime import datetime, date
from typing import Dict, List, Tuple

BASE_FRAUD_SCORE = 0.01
FRAUD_THRESHOLD = 0.7

HIGH_RISK_COUNTRIES = {"NG", "GH", "PK", "BD", "VN"}

HIGH_RISK_MERCHANT_CATEGORIES = {"gambling", "subscriptions", "travel"}

RISK_SEGMENT_WEIGHTS = {
    "low": 0.0,
    "medium": 0.05,
    "high": 0.15
}

def is_night_time(transaction_ts) -> bool:
    hour = transaction_ts.hour
    return hour >= 23 or hour <= 5


def user_age_days(registration_date, transaction_ts) -> int:
    return (transaction_ts.date() - registration_date).days


def compute_fraud_score(context) -> Tuple[float, List[str]]:
    score = BASE_FRAUD_SCORE
    reasons = []

    # Night time transaction
    if is_night_time(context["transaction_ts"]):
        score += 0.10
        reasons.append("night_time_transaction")

    # High risk country
    if context["transaction_country"] in HIGH_RISK_COUNTRIES:
        score += 0.15
        reasons.append("high_risk_country")

    # Transaction velocity (1h)
    if context.get("tx_count_last_1h", 0) > 3:
        score += 0.25
        reasons.append("high_tx_velocity_1h")

    # New device
    if context.get("is_new_device", False):
        score += 0.20
        reasons.append("new_device")

    # Risky merchant category
    if context["merchant_category"] in HIGH_RISK_MERCHANT_CATEGORIES:
        score += 0.10
        reasons.append("risky_merchant_category")

    # Foreign country
    if context.get("user_home_country") != context["transaction_country"]:
        score += 0.10
        reasons.append("foreign_country_transaction")

    age_days = user_age_days(
        context["user_registration_date"],
        context["transaction_ts"]
    )

    if age_days < 7:
        score += 0.15
        reasons.append("very_new_user")
    elif age_days < 30:
        score += 0.10
        reasons.append("new_user")

    risk_segment = context["risk_segment"]

    score += RISK_SEGMENT_WEIGHTS.get(risk_segment, 0.0)

    if risk_segment != "low":
        reasons.append(f"risk_segment_{risk_segment}")


    score = min(score, 1.0)

    return score, reasons


def is_fraud(score: float, threshold: float = FRAUD_THRESHOLD) -> bool:
    return score >= threshold


