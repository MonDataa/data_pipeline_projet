# data_pipeline_projet

## 1) Collecte de donnée (Nifi)

**1) nifi data ingestion :**

**2) nifi regisrtry :**
- 1) Nous avons créé un repository sur Github, puis généré le Personal access token. - 
- 2) Nous allons cloner le repository dans le dossier Nifi-Registry. - 
- 3) Nous modifions le fichier Nifi-registry/conf/providers.xml, et plus précisément la partie flowPersistenceProvider pour mettre nos informations github. Ainsi, les buckets seront enregistré dans notre repository github au lieu de s’enrigster en local. 
- 4) Nous créons un nouveau bucket sur nifi-registry. 
- 5) Nous allons configurer un nouveau registry client sur Nifi pour faire le versionning dans ce bucket. 
- 6) Et finalement, quand on commence le versionning sur un process group, ce dernier va être directement commit dans le Github.

![enter image description here](https://github.com/MonDataa/data_pipeline_projet/blob/master/nifi/nifi_registry_flow.PNG)

## 2) Traitement des données (Spark)
Une fois les données collectées, nous avons procédés à la création d'un dataset regroupant les outputs de nos différents batchs pour créer un fichier final qui regroup toute les variables interresant dans la phase d'analyse.

Nous avons effectués plusieurs opérations moyennant principalement Pyspark sur les données collectés des élections présidentielles de 2022 (Avril) tel que:

- 1) Suppression des doublons et les valeur Null.
- 2) Jointure,Merge.
- 3) Ajout de colonnes (Feature Engineering).
- 4) Renommer les colonnes
- 5)Calcul d'agrégation.

## 3) Orchestration des pipelines de donnée (Airlfow)

Dans le but d'automatiser notre flot de données, nous avons utilisés ce DAGs sur Airflow:

- ce se déclenche chaque heure pour effectuer les traitements qu'on a effectué toutes les operations precedentes :

- 1) Démarrage de spark
- 2) Démarrage de nifi
- 3) traitement de donnée de spark
- 4) Fin spark
- 5) Fin nifi

![enter image description here](https://github.com/MonDataa/data_pipeline_projet/blob/master/airflow/airflow_dag.PNG)
