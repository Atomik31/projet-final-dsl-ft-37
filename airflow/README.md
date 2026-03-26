# 🚀 Déploiement d'Airflow pour votre première pipeline

## 🎯 Objectif

Ce guide explique comment déployer **Airflow** avec **Docker Compose**, et configurer les connexions à **PostgreSQL** et **AWS S3** directement dans l’interface Airflow pour déployer votre première vraie pipeline !

---

## 🛠 Prérequis

- **Docker & Docker Compose** installés
- Un **bucket S3** (à créer)
- Une **base de données PostgreSQL** (à créer via NeonDB, Kubernetes, ou Docker)

---

## 📌 1. Dockerfile (Image Airflow)

Le fichier `Dockerfile` utilisé pour construire l’image Airflow :

```dockerfile
FROM apache/airflow:2.10.4-python3.10

USER root  # Passer en root pour installer les dépendances

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow  # Revenir à l’utilisateur airflow pour la sécurité

# Copier les dépendances Python
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --upgrade --no-build-isolation -r requirements.txt
```

---

## 📌 2. requirements.txt (Dépendances Python)

Les packages requis :

```
apache-airflow-providers-postgres
apache-airflow-providers-amazon
psycopg[binary]
pandas
```

---

## 📌 3. docker-compose.yaml (Déploiement Airflow)

Récuperez le docker-compose du zip.

---

## 📌 4. Démarrage du Serveur Airflow

1. Lancer les conteneurs : (pas besoin de build c'est fait dans le docker compose)

```bash
docker-compose up airflow-init
```

```bash
docker-compose up
```

2. Accéder à Airflow :
   - Ouvrez http://localhost:8080.
   - Connectez-vous avec airflow / airflow.

---

## 📌 5. Configuration des Connexions dans Airflow

### Connexion AWS (S3)

1. Admin > Connections dans Airflow.
2. Créez une connexion avec :

   - Conn Id : aws_default
   - Conn Type : Amazon Web Services
   - AWS Access Key ID : VOTRE_ACCESS_KEY
   - AWS Secret Access Key : VOTRE_SECRET_KEY
   - Extra :

   ```json
   {
     "region_name": "VOTRE_REGION"
   }
   ```

3. Sauvegardez.

### Connexion PostgreSQL

1. Admin > Connections dans Airflow.
2. Créez une connexion avec :
   - Conn Id : postgres_default
   - Conn Type : Postgres
   - Host : VOTRE_HOST
   - Database : VOTRE_BDD
   - Login : VOTRE_UTILISATEUR
   - Password : VOTRE_MOT_DE_PASSE
   - Port : 5432
   - Extra :
   ```json
   {
     "sslmode": "require"
   }
   ```
3. Sauvegardez.

---

## 📌 6. Configuration des Variables d'Environnement dans Airflow

Dans Airflow, les variables d’environnement peuvent être définies directement via l’interface web.

### Étapes :

1. Accédez à l'interface Airflow (http://localhost:8080).
2. Allez dans :
   Admin → Variables → "+" (Ajouter une variable)"
3. Ajoutez les variables suivantes :
   - S3BucketName → Nom du bucket S3
   - WeatherBitApiKey → Clé API WeatherBit
4. Exemple de configuration :
   | Clé | Valeur |
   | :--------------- | :-----------------: |
   | S3BucketName | nom-de-votre-bucket |
   | WeatherBitApiKey | votre-clé-api |

5. Cliquez sur "Enregistrer".

---

## 📌 7. Vous pouvez maintenant trigger votre dags !
