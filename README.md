# 🏭 Machine Monitoring — Maintenance Prédictive Temps Réel

> Pipeline de données temps réel pour la détection d'anomalies sur des machines industrielles, de l'ingestion des capteurs jusqu'aux tableaux de bord, déployé sur Kubernetes.

![CI](https://github.com/Soumiaelhaffari25/machine-monitoring-realtime/actions/workflows/ci.yml/badge.svg)
![CD](https://github.com/Soumiaelhaffari25/machine-monitoring-realtime/actions/workflows/cd.yml/badge.svg)

---

## 📋 Aperçu

Ce projet met en œuvre une chaîne de traitement de données **temps réel** pour surveiller un parc de machines industrielles équipées de capteurs (température, vibration, pression, courant). Il détecte les signes de défaillance **avant la panne** grâce à un moteur de règles statistiques, et présente les résultats sur des tableaux de bord métier et techniques.

La chaîne complète — de la production des données à la visualisation — fonctionne aussi bien **en local** (Docker Compose) que **déployée sur Kubernetes**, avec une **intégration continue** complète.

**Données réalistes :** le simulateur de capteurs est calibré sur le [NASA Bearing Dataset](https://www.kaggle.com/datasets/vinayak123tyagi/bearing-dataset) — des roulements réels poussés jusqu'à la défaillance. Les statistiques de vibration réelles ont été extraites (roulement sain ≈ 0.078g RMS, dégradé ≈ 0.20g+) pour reproduire des signatures authentiques, incluant du bruit normal, des pics soudains et des dérives progressives (usure).

---

## 🏗️ Architecture

```
┌──────────┐   ┌─────────┐   ┌─────────┐   ┌──────────┐   ┌──────────────┐   ┌──────────┐
│ Capteurs │──▶│  Kafka  │──▶│  Spark  │──▶│ MongoDB  │──▶│  API FastAPI │──▶│ Grafana  │
│ (simulés)│   │ (KRaft) │   │Streaming│   │          │   │   (pont)     │   │ Dashboard│
└──────────┘   └─────────┘   └─────────┘   └──────────┘   └──────────────┘   └──────────┘
                    │              │
              ┌─────────────┐  ┌──────────────┐
              │   Schema    │  │  Prometheus  │──▶ Alertmanager
              │  Registry   │  │  (métriques) │──▶ Dashboard technique
              └─────────────┘  └──────────────┘
```

**Flux de données :**
1. Un **producteur** génère des mesures et les publie dans **Kafka** au format **Avro**, dont le contrat est validé par le **Schema Registry**. Le partitionnement Kafka se fait par `machine_id`.
2. **Spark Structured Streaming** consomme le flux, décode l'Avro, applique des **fenêtres temporelles (tumbling 1 min)** avec watermark, détecte les anomalies via un **moteur de 3 règles**, et route les messages corrompus vers une **Dead Letter Queue**.
3. Les anomalies (upsert) et toutes les mesures (historique) sont écrites dans **MongoDB**, avec un **index TTL** de purge automatique.
4. Un **pont API FastAPI** traduit les requêtes de **Grafana** vers MongoDB ; **Prometheus** scrape les métriques techniques des composants.

---

## 🔬 Détails techniques

### Moteur de détection d'anomalies

Trois règles complémentaires produisent chacune un signal (0 ou 1), combinés en un score pondéré :

| Règle | Principe | Poids |
|---|---|---|
| **Seuil physique** | Dépassement d'une borne critique connue (temp > 90°C, vibration > 0.20g, pression > 7 bar, courant > 50 A) | 0.4 |
| **Z-score** | Écart statistique : \|Z\| > 3 par rapport à la moyenne/écart-type de la fenêtre | 0.4 |
| **Dérive** | Montée progressive : valeur > moyenne × 1.30 tout en restant sous le seuil (détection d'usure précoce) | 0.2 |

`score = Σ (poids × signal)`. Une cascade de priorité (seuil > z-score > dérive) désigne la règle principale pour l'agrégation. La logique métier est isolée dans `rules_logic.py` (fonctions pures) pour un test unitaire rapide, et appliquée dans `rules.py` côté Spark.

### Garanties de robustesse

- **Dead Letter Queue** : les messages dont le décodage Avro échoue (`machine_id IS NULL`) sont routés vers un topic `dead-letter` au lieu de faire échouer le pipeline.
- **Exactly-once** : les checkpoints Spark permettent une reprise sans perte ni doublon après un arrêt (testé : arrêt au batch N, reprise au batch N+1).
- **Idempotence** : écriture des anomalies en **upsert** sur la clé `(machine_id, sensor, fenêtre)` — un redémarrage ne crée pas de doublons.
- **Gestion de la volumétrie** : index TTL MongoDB (purge auto après 1h) + index composés pour les requêtes du dashboard.

### Observabilité

- **Dashboard métier** (source MongoDB via API) : cartes clés, statut par machine, courbes de capteurs, répartition des anomalies par règle et par capteur, journal des anomalies avec sévérité.
- **Dashboard technique** (source Prometheus) : débit d'ingestion vs traitement, activité des topics Kafka, santé des services (`up`), volume de la DLQ.
- **Alerting** : règles Prometheus (`ServiceDown`, `DeadLetterQueueGrowing`, `NoDataIngested`) routées vers Alertmanager.

---

## 🛠️ Stack technique

| Domaine | Technologies |
|---|---|
| **Streaming** | Apache Kafka (KRaft, sans Zookeeper), Apache Spark 3.5.1 (Structured Streaming) |
| **Stockage** | MongoDB 7 (index TTL, upsert) |
| **Sérialisation** | Avro, Confluent Schema Registry |
| **Visualisation** | Grafana (plugin JSON datasource) |
| **Observabilité** | Prometheus, Alertmanager, kafka-exporter, mongodb-exporter |
| **API** | FastAPI + Uvicorn |
| **Orchestration** | Kubernetes (Kind), opérateur Strimzi pour Kafka |
| **CI/CD** | GitHub Actions, GitHub Container Registry (GHCR) |
| **Langage** | Python 3.11 |

---

## 🚀 Installation et lancement

### Prérequis
- Docker Desktop
- Python 3.11
- Java 17 (requis pour l'exécution locale de Spark — Spark 3.5 n'est pas compatible Java 18+)
- kubectl et [Kind](https://kind.sigs.k8s.io/) (pour le déploiement Kubernetes)

### Cloner le projet
```bash
git clone https://github.com/Soumiaelhaffari25/machine-monitoring-realtime.git
cd machine-monitoring-realtime
```

### Option 1 — Lancement local (Docker Compose)

```bash
# 1. Infrastructure (Kafka, Schema Registry, MongoDB, Grafana, Prometheus)
docker compose up -d

# 2. Environnement Python
python -m venv .venv
.venv\Scripts\activate          # Windows  (source .venv/bin/activate sur Linux/Mac)
pip install -r requirements.txt

# 3. Composants applicatifs (terminaux séparés)
python producer/producer.py                                    # producteur de mesures
python spark_job/streaming_job.py                              # job Spark de traitement
uvicorn api.grafana_bridge:app --host 0.0.0.0 --port 8000     # pont API pour Grafana
```

**Interfaces :** Grafana → http://localhost:3000 (admin/admin) · Prometheus → http://localhost:9090

**Note Windows :** l'exécution locale de Spark nécessite `winutils` (Hadoop) et `JAVA_HOME` pointant vers Java 17. Le déploiement Kubernetes (option 2) évite ces contraintes en s'exécutant dans des conteneurs Linux.

### Option 2 — Déploiement Kubernetes (Kind)

```bash
# 1. Créer le cluster (mappe Grafana/Prometheus sur des ports locaux)
kind create cluster --config k8s/kind-config.yaml

# 2. Construire et charger les images dans le cluster
docker build -t machine-monitoring/producer:v1 -f producer/Dockerfile .
docker build -t machine-monitoring/spark-job:v1 -f spark_job/Dockerfile .
kind load docker-image machine-monitoring/producer:v1 --name machine-monitoring
kind load docker-image machine-monitoring/spark-job:v1 --name machine-monitoring

# 3. Infrastructure — MongoDB
kubectl apply -f k8s/mongodb.yaml

# 4. Infrastructure — Kafka via l'opérateur Strimzi
kubectl create namespace kafka
kubectl apply -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka
kubectl wait --for=condition=Ready pod -l name=strimzi-cluster-operator -n kafka --timeout=180s
kubectl apply -f k8s/kafka.yaml
kubectl wait kafka/my-cluster --for=condition=Ready -n kafka --timeout=300s

# 5. Infrastructure — Schema Registry
kubectl apply -f k8s/schema-registry.yaml

# 6. Applications
kubectl apply -f k8s/producer.yaml
kubectl apply -f k8s/spark-job.yaml

# 7. Vérifier le pipeline
kubectl get pods
kubectl logs -l app=spark-job --tail=20        # doit montrer les batchs traités
```

**Communication inter-services :** les composants se joignent par nom de service Kubernetes (`mongodb`, `schemaregistry`, `my-cluster-kafka-bootstrap.kafka.svc.cluster.local`). Les applications sont configurées via variables d'environnement (`KAFKA_BOOTSTRAP`, `MONGO_URI`, `SCHEMA_REGISTRY_URL`), avec des valeurs par défaut `localhost` pour l'exécution locale — le même code fonctionne dans les deux environnements.

---

## 🧪 Tests

Le moteur de détection est couvert par des tests unitaires sur la logique pure (sans dépendance Spark, exécution en millisecondes) :

```bash
pip install pytest
pytest tests/ -v
```

Les tests couvrent : les seuils physiques par capteur, le calcul du Z-score (dont le cas limite σ=0), la détection de dérive, et le score combiné avec la cascade de priorité.

---

## 🔄 CI/CD

Deux workflows GitHub Actions :

- **CI** (`ci.yml`) — à chaque push : linting (flake8), tests (pytest), build de validation des images Docker.
- **CD** (`cd.yml`) — à chaque push : build et **publication des images** sur GitHub Container Registry (`ghcr.io`). Le déploiement Kubernetes est fourni en template (activable avec un cluster distant accessible).

---

## 📁 Structure du projet

```
machine-monitoring-realtime/
├── producer/                    # Simulateur de capteurs (calibré NASA)
│   ├── producer.py              # Génère et publie les mesures dans Kafka (Avro)
│   ├── extract_stats.py         # Extraction des stats du dataset NASA
│   ├── calibration.json         # Stats de calibration (sain vs dégradé)
│   ├── schemas/reading.avsc     # Contrat Avro des mesures
│   └── Dockerfile
├── spark_job/                   # Traitement Spark (streaming + détection)
│   ├── streaming_job.py         # Pipeline final : Kafka → Avro → fenêtres → règles → sinks
│   ├── rules.py                 # Moteur de détection (Spark)
│   ├── rules_logic.py           # Logique pure testable
│   ├── mongo_sink.py            # Écriture MongoDB (upsert anomalies, insert mesures)
│   ├── setup_indexes.py         # Création des index MongoDB (TTL + performance)
│   ├── inject_bad_message.py    # Test de la Dead Letter Queue
│   ├── read_kafka_raw.py        # Étapes de développement incrémental :
│   ├── read_kafka_avro.py       #   lecture brute → décodage Avro →
│   ├── read_with_dlq.py         #   DLQ → agrégations fenêtrées →
│   ├── windowed_stats.py        #   détection, construits par paliers
│   ├── detect_anomalies.py
│   ├── test_spark.py            # Vérification de l'environnement Spark
│   └── Dockerfile
├── api/
│   └── grafana_bridge.py        # Pont FastAPI MongoDB ↔ Grafana
├── dashboards/                  # Dashboards Grafana
│   ├── grafana_business.json    # Dashboard métier (anomalies)
│   └── grafana_technical.json   # Dashboard technique (santé du pipeline)
├── prometheus/                  # Observabilité
│   ├── prometheus.yml           # Config de scraping
│   ├── alert_rules.yml          # Règles d'alerte
│   └── alertmanager.yml         # Config des notifications
├── k8s/                         # Manifests Kubernetes
│   ├── kind-config.yaml         # Configuration du cluster
│   ├── mongodb.yaml
│   ├── kafka.yaml               # Cluster Kafka (Strimzi)
│   ├── schema-registry.yaml
│   ├── producer.yaml
│   └── spark-job.yaml
├── tests/
│   └── test_rules.py            # Tests unitaires du moteur de détection
├── .github/workflows/           # CI/CD
│   ├── ci.yml
│   └── cd.yml
├── docker-compose.yml           # Infrastructure locale
├── requirements.txt
└── .flake8
```

---

## 📊 Aperçu des dashboards

<!-- Décommente après avoir ajouté tes captures dans un dossier docs/ :
### Dashboard métier
![Dashboard métier](docs/dashboard_business.png)

### Dashboard technique
![Dashboard technique](docs/dashboard_technical.png)
-->

*[À COMPLÉTER : insérer les captures des dashboards métier et technique]*

---

## 🧭 Défis techniques relevés

Quelques problèmes concrets résolus au cours du projet, illustrant la démarche de débogage :
- **Compatibilité des versions** (Python 3.11 / Java 17 / Spark 3.5) pour éviter les erreurs de sérialisation.
- **Collision de variables Kubernetes/Confluent** : le Schema Registry crashait car Kubernetes injecte une variable `SCHEMA_REGISTRY_PORT` interprétée à tort par Confluent — résolu via `enableServiceLinks: false`.
- **Connecteurs Kafka en conteneur** : pré-intégration des JARs Spark-Kafka dans l'image pour éviter un téléchargement au démarrage.
- **Déduplication** : passage d'un `insert` à un `upsert` pour éliminer les anomalies dupliquées entre fenêtres.

---

## 👤 Auteur

**Soumia El Haffari**

- LinkedIn : [soumia-elhaffari](https://www.linkedin.com/in/soumia-elhaffari-080237320/)
- Email : soumiaelhaffari2525@gmail.com

---

*Projet personnel — pipeline de données temps réel de bout en bout, du capteur au tableau de bord, déployé sur Kubernetes.*
