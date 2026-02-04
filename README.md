├── README.md                               
├── data/
│   ├── batiments.csv
│   ├── consommations_raw.csv
│   ├── meteo_raw.csv
│   └── tarifs_energie.csv
├── notebooks/
│   ├── 01_exploration_spark.ipynb
│   ├── 02_nettoyage_spark.py
│   ├── 03_agregations_spark.ipynb
│   ├── 04_nettoyage_meteo_pandas.ipynb
│   ├── 05_fusion_enrichissement.ipynb
│   ├── 06_statistiques_descriptives.ipynb
│   ├── 07_analyse_correlations.ipynb
│   ├── 08_detection_anomalies.ipynb
│   ├── 09_visualisations_matplotlib.ipynb
│   ├── 10_visualisations_seaborn.ipynb
│   └── 11_dashboard_executif.ipynb
├── output/
│   ├── consommations_clean_partitionne/               
│   ├── consommations_agregees.parquet/
│   ├── meteo_clean.csv
│   ├── consommations_enrichies_final.csv
│   ├── consommations_enrichies.parquet/
│   ├── dataset_final_analyse.parquet/                    
│   └── figures/
└── requirements.txt


Je suis sincèrement désolée, je n'ai pas réussi à terminer le projet. 

J'ai eu énormément de soucis et j'ai perdu plusieurs heureus pour spark et le fichier extrémement volumineux de 7.7 millions de lignes.
Il fallait attendre 30 minutes pour lire le script (en espérant qu'il n'y ait pas d'erreur).
Par la suite j'ai essayé d'avancer au plus vite, mais ça n'a pas suffit. 
Je suis navrée également pour le franglais, des colonnes sont en français et d'autres en anglais...

Certains fichiers devant être en .ipynb sont finalement en .py. 
En effet, il fallait être en docker pour pouvoir partitionner et lire les parquet mais je n'ai jamais réussi à utiliser docker avec le .ipynb. Donc je suis restée en .py pour essayer de faire un travail correct. 

