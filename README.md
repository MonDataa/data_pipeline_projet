# data_pipeline_projet

***TRAVAUX EFFECTUÉS***

***Réalisé par : Mounsif Elkhiyar , Achraf ATTARY , Basel ALASSAAD***

## 1) Collecte de donnée (Nifi)

**1) nifi data ingestion :** l'objectif dans cette phase et de collecter les données dans differentes source ,dans notre cas on va travaillé  deux fichier excel d'Election présidentielle des 10 et 24 avril 2022,aprés on va créer des processus pour extraire les données ,transformation des données et qu'elles sont accessibles dans differentes source (HDFS,local ou meme l'extraire via le producer de kafka),implementer une strategie de validation des données.


Donc pour realisé cette tache nous avons crées deux groupes :

![enter image description here](https://github.com/MonDataa/data_pipeline_projet/blob/master/nifi/nifi_group.PNG)

`Groupe 1` : nous avons créer ce groupe pour convertir les fichiers excel en csv :

![enter image description here](https://github.com/MonDataa/data_pipeline_projet/blob/master/nifi/nifi_flow1.PNG)

`Groupe 2` : ce groupe sert a etablir un systeme de validation des fichiers csv graces aux processus (ValidationCSV) aprés on a utilisé Kafka sur nifi ,on a travaillé par les processeurs consummeKafka et publishKafka pour consommer nos données de nifi dans deux topics ( valid et invalid data ) et les publier dans un fichier (HDFS,local)

![enter image description here](https://github.com/MonDataa/data_pipeline_projet/blob/master/nifi/nifi_flow2.PNG)

![enter image description here](https://github.com/MonDataa/data_pipeline_projet/blob/master/nifi/resultat_hdfs.PNG)

**2) nifi regisrtry :**
- 1) Nous avons créé un repository sur `Github`, puis généré le Personal access token. - 
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
- 3) Ajoutes des colonnes (Feature Engineering).
- 4) Renommer les colonnes
- 5) Calcul d'agrégation.

## 3) Orchestration des pipelines de donnée (Airlfow)

Dans le but d'automatiser notre flot de données, nous avons utilisés ce DAGs sur Airflow:

- ce se déclenche chaque heure pour effectuer les traitements qu'on a effectué toutes les operations precedentes :

- 1) Démarrage `spark`
- 2) Démarrage `nifi`
- 3) traitement de donnée `spark`
- 4) Fin `spark`
- 5) Fin `nifi`

![enter image description here](https://github.com/MonDataa/data_pipeline_projet/blob/master/airflow/airflow_dag.PNG)
