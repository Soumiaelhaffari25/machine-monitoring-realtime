PHYSICAL_THRESHOLDS = {
    "temperature": 90.0,
    "vibration": 0.20,
    "pressure": 7.0,
    "current": 50.0,
}
ZSCORE_THRESHOLD = 3.0
DRIFT_RATIO = 1.30
WEIGHT_PHYSICAL = 0.4
WEIGHT_ZSCORE = 0.4
WEIGHT_DRIFT = 0.2


def is_physical_anomaly(sensor: str, value: float) -> bool:
    """True si la valeur depasse le seuil physique du capteur."""
    threshold = PHYSICAL_THRESHOLDS.get(sensor)
    if threshold is None:
        return False
    return value > threshold


def compute_zscore(value: float, mu: float, sigma: float) -> float:
    """Calcule le Z-score. Retourne 0 si sigma invalide."""
    if sigma is None or sigma <= 0:
        return 0.0
    return (value - mu) / sigma


def is_zscore_anomaly(value: float, mu: float, sigma: float) -> bool:
    """True si |Z-score| depasse le seuil."""
    return abs(compute_zscore(value, mu, sigma)) > ZSCORE_THRESHOLD


def is_drift(value: float, mu: float, threshold: float) -> bool:
    """True si derive : monte au-dessus de mu*1.30 mais reste sous le seuil."""
    if mu is None or mu <= 0:
        return False
    return value > mu * DRIFT_RATIO and value <= threshold


def compute_score(sensor: str, value: float, mu: float, sigma: float) -> dict:
    """Calcule le score complet et la regle principale (logique pure)."""
    threshold = PHYSICAL_THRESHOLDS.get(sensor, float("inf"))
    sig_physical = 1.0 if is_physical_anomaly(sensor, value) else 0.0
    sig_zscore = 1.0 if is_zscore_anomaly(value, mu, sigma) else 0.0
    sig_drift = 1.0 if is_drift(value, mu, threshold) else 0.0

    score = (WEIGHT_PHYSICAL * sig_physical
             + WEIGHT_ZSCORE * sig_zscore
             + WEIGHT_DRIFT * sig_drift)

    if sig_physical == 1.0:
        rule = "Seuil physique"
    elif sig_zscore == 1.0:
        rule = "Z-score"
    elif sig_drift == 1.0:
        rule = "Derive"
    else:
        rule = "Aucune"

    return {"score": round(score, 4), "is_anomaly": score > 0, "rule": rule}
