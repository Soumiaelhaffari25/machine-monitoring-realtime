"""
Extrait les statistiques reelles de vibration du NASA Bearing Dataset (Set 2).
Calcule le RMS par fichier pour Bearing 1 (celui qui tombe en panne),
puis derive les plages 'normal' (debut) et 'anomalie' (fin de vie).
Sauvegarde le resultat dans calibration.json pour calibrer le simulateur.
"""

import json
from pathlib import Path

import numpy as np

# Set 2 : Bearing 1 developpe une panne de bague exterieure en fin de test
DATA_DIR = Path(__file__).parent.parent / "data" / "2nd_test" / "2nd_test"
BEARING_COLUMN = 0          # colonne 0 = Bearing 1
OUTPUT = Path(__file__).parent / "calibration.json"


def rms(values: np.ndarray) -> float:
    """Root Mean Square : amplitude efficace d'un signal oscillant."""
    return float(np.sqrt(np.mean(np.square(values))))


def main():
    # Les fichiers sont nommes par horodatage -> le tri alphabetique = ordre chronologique
    files = sorted(DATA_DIR.iterdir())
    print(f"{len(files)} fichiers trouves dans le Set 2.")

    rms_series = []
    for i, f in enumerate(files):
        data = np.loadtxt(f)                 # charge les 20480 lignes x 4 colonnes
        column = data[:, BEARING_COLUMN]     # on isole Bearing 1
        rms_series.append(rms(column))
        if i % 100 == 0:
            print(f"  {i}/{len(files)} fichiers traites...", end="\r")

    rms_series = np.array(rms_series)
    print(f"\nTraitement termine. RMS min={rms_series.min():.4f}, max={rms_series.max():.4f}")

    # Etat sain : les 100 premiers fichiers (debut de vie du roulement)
    healthy = rms_series[:100]
    # Etat degrade : les 100 derniers fichiers (juste avant la panne)
    degraded = rms_series[-100:]

    calibration = {
        "vibration": {
            "source": "NASA Bearing Dataset Set 2, Bearing 1 (RMS accel en g)",
            "normal_mean": round(float(healthy.mean()), 4),
            "normal_std": round(float(healthy.std()), 4),
            "anomaly_mean": round(float(degraded.mean()), 4),
            "anomaly_std": round(float(degraded.std()), 4),
        }
    }

    OUTPUT.write_text(json.dumps(calibration, indent=2), encoding="utf-8")
    print(f"\nCalibration sauvegardee dans {OUTPUT}")
    print(json.dumps(calibration, indent=2))


if __name__ == "__main__":
    main()