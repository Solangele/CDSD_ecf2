from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W 

# 1. Initialisation
spark = SparkSession.builder.appName("Fusion_Enrichissement").getOrCreate()

# 2. Chargement des données
path_conso = "/output/consommations_clean_partitionne"
path_meteo = "/output/meteo_clean.csv" 
path_batiments = "/data_ecf/batiments.csv"
path_tarifs = "/data_ecf/tarifs_energie.csv" 

df_conso = spark.read.parquet(path_conso)
df_meteo = spark.read.option("header", "true").option("inferSchema", "true").csv(path_meteo)
df_batiments = spark.read.option("header", "true").option("inferSchema", "true").csv(path_batiments)
df_tarifs = spark.read.option("header", "true").option("inferSchema", "true").csv(path_tarifs)

# Préparation technique
df_conso = df_conso.withColumn("timestamp_hour", F.to_timestamp(F.concat(F.col("date"), F.lit(" "), F.col("hour"), F.lit(":00:00"))))
df_meteo = df_meteo.withColumn("timestamp", F.to_timestamp("timestamp"))

# --- ÉTAPE 3 : Fusions ---
print("Fusion des données en cours...")

# 1. Joindre Bâtiments
df_step1 = df_conso.join(
    df_batiments.select("batiment_id", "surface_m2", "nb_occupants_moyen", "type", "nom", "commune"), 
    on="batiment_id", 
    how="left"
)

# 2. Joindre Météo (on utilise df_step1)
cols_a_supp = ["date", "hour", "year", "month", "jour", "mois", "annee", "heure"]
df_meteo_clean = df_meteo

for c in cols_a_supp:
    if c in df_meteo.columns:
        df_meteo_clean = df_meteo_clean.drop(c)

print("Jointure avec la météo (nettoyée des colonnes redondantes)...")
df_step2 = df_step1.join(
    df_meteo_clean, 
    (df_step1.commune == df_meteo_clean.commune) & (df_step1.timestamp_hour == df_meteo_clean.timestamp), 
    how="left"
).drop(df_meteo_clean.commune).drop(df_meteo_clean.timestamp)

# 3. Joindre Tarifs (on utilise df_step2)
# Vérifie bien que la colonne prix dans tarifs_energie.csv est 'tarif_unitaire'
df_step3 = df_step2.join(df_tarifs, on="type_energie", how="left")

print("Calcul des indicateurs de performance...")

# --- ÉTAPE 4 : Calcul des KPI ---
# Utilisation de df_step3 qui contient tout (Conso + Batiment + Meteo + Tarif)
df_final = df_step3.withColumn(
    "cout_consommation", F.round(F.col("conso_mean") * F.col("tarif_unitaire"), 2)
).withColumn(
    "conso_par_occupant", F.round(F.col("conso_mean") / F.col("nb_occupants_moyen"), 4)
).withColumn(
    "conso_par_m2", F.round(F.col("conso_mean") / F.col("surface_m2"), 4)
).withColumn(
    "ipe", F.round(F.col("conso_mean") / F.col("surface_m2"), 4)
)

# Calcul de l'écart à la moyenne
window_type = W.partitionBy("type")
df_final = df_final.withColumn(
    "moyenne_categorie", F.avg("conso_mean").over(window_type)
).withColumn(
    "ecart_a_la_moyenne", F.round(F.col("conso_mean") - F.col("moyenne_categorie"), 2)
).drop("moyenne_categorie")

# --- SAUVEGARDE ---
output_path = "/output/consommations_enrichies"
print(f"Sauvegarde du dataset final dans {output_path}...")

df_final.write.mode("overwrite").parquet(f"{output_path}.parquet")
df_final.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}.csv")

print("Fusion et enrichissement terminés !")
spark.stop()