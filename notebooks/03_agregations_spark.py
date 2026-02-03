from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ECF_Final_Local") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

r
PATH_PARQUET = "/output/consommations_clean/par_heure"
PATH_BATIMENTS = "/data_ecf/batiments.csv"

print("--- Chargement des données ---")
df_conso = spark.read.parquet(PATH_PARQUET)
df_bat = spark.read.option(
    "header", 
    "true").option(
        "inferSchema", 
        "true").csv(PATH_BATIMENTS)

# 1. Jointure et Intensité
df_final = df_conso.join(df_bat, on="batiment_id")
df_final = df_final.withColumn("intensite", F.col("conso_mean") / F.col("surface_m2"))

# 2. Médiane et Hors-normes
median_val = df_final.stat.approxQuantile("intensite", [0.5], 0.05)[0]
df_hors_norme = df_final.filter(F.col("intensite") > (median_val * 3))

# 3. Agrégation par commune
df_communes = df_final.groupBy("commune").agg(F.sum("conso_mean").alias("total_kwh"))

print(f"Nombre de batiments hors-normes : {df_hors_norme.count()}")
df_communes.show(10)

# 4. Vue SQL
df_final.createOrReplaceTempView("v_consommations")
print("Analyse terminée avec succès.")

spark.stop()