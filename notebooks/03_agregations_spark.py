from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialisation
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ECF_Final_Analysis") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# --- 1. CHARGEMENT ---
PATH_PARQUET = "/output/consommations_clean/par_heure"
PATH_BATIMENTS = "/data_ecf/batiments.csv"

print("--- Chargement des données ---")
df_conso = spark.read.parquet(PATH_PARQUET)
df_bat = spark.read.option("header", "true").option("inferSchema", "true").csv(PATH_BATIMENTS)

# --- 2. JOINTURE ET CALCULS ---
df_final = df_conso.join(df_bat, on="batiment_id")

# Calcul de l'intensité énergétique
df_final = df_final.withColumn("intensite", F.col("conso_mean") / F.col("surface_m2"))

# --- 3. MÉDIANE PAR TYPE (Correction nom de colonne : 'type') ---
print("Calcul des médianes par type...")
window_spec = Window.partitionBy("type")
df_final = df_final.withColumn("mediane_type", F.percentile_approx("intensite", 0.5).over(window_spec))

# --- 4. SAUVEGARDE DU LIVRABLE ---
# Utilisation du chemin relatif direct pour éviter le bug du ".."
print("Sauvegarde du fichier Parquet...")
df_final.write.mode("overwrite").parquet("output/consommations_agregees.parquet")

# --- 5. DÉMONSTRATION SPARK SQL (Correction noms : 'commune' et 'type') ---
df_final.createOrReplaceTempView("v_conso")

print("\n--- REQUÊTE 1 : TOP 5 COMMUNES ÉNERGIVORES ---")
spark.sql("""
    SELECT commune, ROUND(SUM(conso_mean), 2) as total_kwh
    FROM v_conso
    GROUP BY commune
    ORDER BY total_kwh DESC
    LIMIT 5
""").show()

print("\n--- REQUÊTE 2 : INTENSITÉ MOYENNE PAR TYPE ---")
spark.sql("""
    SELECT type, ROUND(AVG(intensite), 2) as intensite_moyenne
    FROM v_conso
    GROUP BY type
    ORDER BY intensite_moyenne DESC
""").show()

print("\n--- REQUÊTE 3 : ÉCHANTILLON DES HORS-NORMES ---")
spark.sql("""
    SELECT batiment_id, commune, type, ROUND(intensite, 2) as intensite, ROUND(mediane_type, 2) as mediane_type
    FROM v_conso
    WHERE intensite > (mediane_type * 3)
    ORDER BY intensite DESC
""").show(10)

print("Analyse terminée avec succès.")
spark.stop()