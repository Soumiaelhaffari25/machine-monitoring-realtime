"""
Moteur de detection par regles statistiques (sans Machine Learning).
Chaque regle est une fonction pure : elle prend un DataFrame et ajoute
une colonne de signal (1 = regle declenchee, 0 = normale).
Les signaux ponderes forment un score d'anomalie final.

Regles implementees :
  - seuil physique : valeur au-dela d'une borne critique connue
  - Z-score        : ecart > 3 sigma de la moyenne glissante
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, abs as sabs, lit

# --- Seuils physiques critiques par capteur ---
# temperature en C, vibration en g (calibree NASA), pression en bar, courant en A
PHYSICAL_THRESHOLDS = {
    "temperature": 90.0,
    "vibration": 0.20,
    "pressure": 7.0,
    "current": 50.0,
}

# Seuil du Z-score : au-dela de 3 ecarts-types, la valeur est statistiquement anormale
ZSCORE_THRESHOLD = 3.0
# Marge de derive : valeur depassant la moyenne de fenetre de +30%
DRIFT_RATIO = 1.30

# Poids de chaque regle dans le score final
WEIGHT_PHYSICAL = 0.4
WEIGHT_ZSCORE = 0.4
WEIGHT_DRIFT = 0.2

def rule_physical_threshold(df: DataFrame) -> DataFrame:
    """
    Regle du seuil physique.
    Compare la valeur a la borne critique connue du capteur.
    Ajoute la colonne 'signal_physical' (1.0 si depassement, sinon 0.0).
    """
    # On construit une expression : selon le capteur, quel seuil appliquer ?
    threshold_col = lit(None).cast("double")
    for sensor, thr in PHYSICAL_THRESHOLDS.items():
        threshold_col = when(col("sensor") == sensor, lit(thr)).otherwise(threshold_col)

    return df.withColumn("threshold", threshold_col).withColumn(
        "signal_physical",
        when(col("value") > col("threshold"), lit(1.0)).otherwise(lit(0.0)),
    )


def rule_zscore(df: DataFrame) -> DataFrame:
    """
    Regle du Z-score.
    Z = (valeur - moyenne) / ecart-type. Anormal si |Z| > seuil.
    Necessite les colonnes 'mu' et 'sigma' (issues des agregations fenetrees).
    Ajoute les colonnes 'zscore' et 'signal_zscore'.
    """
    # Si sigma est null ou nul (pas assez de donnees), Z-score = 0 (pas d'anomalie)
    zscore = when(
        (col("sigma").isNotNull()) & (col("sigma") > 0),
        (col("value") - col("mu")) / col("sigma"),
    ).otherwise(lit(0.0))

    return df.withColumn("zscore", zscore).withColumn(
        "signal_zscore",
        when(sabs(col("zscore")) > ZSCORE_THRESHOLD, lit(1.0)).otherwise(lit(0.0)),
    )
    
def rule_drift(df: DataFrame) -> DataFrame:
    """
    Regle de derive (tendance croissante / usure).
    Se declenche si la derniere valeur monte nettement au-dessus de la
    moyenne de fenetre (>+30%), MAIS reste sous le seuil physique
    (sinon c'est la regle du seuil qui gere).
    Necessite 'mu' (moyenne de fenetre) et 'threshold' (seuil physique).
    Ajoute la colonne 'signal_drift'.
    """
    # Condition : value > mu * 1.30  ET  value <= seuil physique
    is_drift = (
        (col("mu").isNotNull())
        & (col("mu") > 0)
        & (col("value") > col("mu") * DRIFT_RATIO)
        & (col("value") <= col("threshold"))
    )
    return df.withColumn(
        "signal_drift",
        when(is_drift, lit(1.0)).otherwise(lit(0.0)),
    )


def compute_anomaly_score(df: DataFrame) -> DataFrame:
    """
    Applique toutes les regles et calcule le score d'anomalie pondere.
    score = somme(poids * signal) pour chaque regle.
    Ajoute 'anomaly_score', 'is_anomaly' et 'rule' (regle principale declenchee).
    """
    df = rule_physical_threshold(df)   # cree 'threshold' et 'signal_physical'
    df = rule_zscore(df)               # cree 'zscore' et 'signal_zscore'
    df = rule_drift(df)                # cree 'signal_drift' (utilise 'threshold')

    score = (
        WEIGHT_PHYSICAL * col("signal_physical")
        + WEIGHT_ZSCORE * col("signal_zscore")
        + WEIGHT_DRIFT * col("signal_drift")
    )

    df = df.withColumn("anomaly_score", score).withColumn(
        "is_anomaly", col("anomaly_score") > 0
    )

    # Regle principale declenchee (pour le camembert 'anomalies by rule')
    rule = (
        when(col("signal_physical") == 1.0, lit("Seuil physique"))
        .when(col("signal_zscore") == 1.0, lit("Z-score"))
        .when(col("signal_drift") == 1.0, lit("Derive"))
        .otherwise(lit("Aucune"))
    )
    return df.withColumn("rule", rule)