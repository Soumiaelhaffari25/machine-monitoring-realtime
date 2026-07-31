"""
Tests de la logique metier des regles (rapides, sans Spark).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "spark_job"))

from rules_logic import (
    is_physical_anomaly,
    compute_zscore,
    is_zscore_anomaly,
    is_drift,
    compute_score,
)


def test_seuil_physique_temperature_depassee():
    assert is_physical_anomaly("temperature", 120.0) is True


def test_seuil_physique_temperature_normale():
    assert is_physical_anomaly("temperature", 45.0) is False


def test_seuil_physique_vibration():
    assert is_physical_anomaly("vibration", 0.25) is True


def test_zscore_calcul():
    # (100 - 50) / 10 = 5
    assert compute_zscore(100.0, 50.0, 10.0) == 5.0


def test_zscore_anomalie():
    assert is_zscore_anomaly(100.0, 50.0, 10.0) is True   # Z=5 > 3


def test_zscore_normal():
    assert is_zscore_anomaly(60.0, 50.0, 10.0) is False   # Z=1 < 3


def test_zscore_sigma_nul():
    # sigma=0 ne doit pas planter, zscore = 0
    assert compute_zscore(60.0, 50.0, 0.0) == 0.0
    assert is_zscore_anomaly(60.0, 50.0, 0.0) is False


def test_derive():
    # value=10 > mu(7)*1.3=9.1 et <= seuil pression 7? Non, prenons courant
    # mu=30, seuil courant=50 : 45 > 30*1.3=39 et 45<=50 -> derive
    assert is_drift(45.0, 30.0, 50.0) is True


def test_derive_mais_depasse_seuil():
    # 55 > 39 mais 55 > 50 (seuil) -> pas derive (c'est le seuil qui gere)
    assert is_drift(55.0, 30.0, 50.0) is False


def test_score_combine_deux_regles():
    # temperature 120 : physique (>90) + zscore (Z=(120-45)/10=7.5>3) = 0.8
    result = compute_score("temperature", 120.0, 45.0, 10.0)
    assert result["score"] == 0.8
    assert result["is_anomaly"] is True
    assert result["rule"] == "Seuil physique"


def test_score_aucune_anomalie():
    result = compute_score("temperature", 45.0, 45.0, 10.0)
    assert result["score"] == 0.0
    assert result["is_anomaly"] is False
    assert result["rule"] == "Aucune"