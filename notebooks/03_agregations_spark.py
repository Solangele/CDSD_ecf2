from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialisation
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ECF_Final_Analysis") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()


PATH_ENRICHI = "/output/consommations_enrichies.parquet"

print("--- Chargement des données enrichies ---")
df_final = spark.read.parquet(PATH_ENRICHI)


print("Calcul des médianes de performance par catégorie...")
window_spec = Window.partitionBy("type")

df_final = df_final.withColumn("mediane_type", F.percentile_approx("ipe", 0.5).over(window_spec))


print("Sauvegarde du fichier agrégé pour le rapport...")
df_final.write.mode("overwrite").parquet("/output/consommations_agregees.parquet")


df_final.createOrReplaceTempView("v_conso")

print("\n--- REQUÊTE 1 : TOP 5 COMMUNES ÉNERGIVORES ---")

spark.sql("""
    SELECT commune, ROUND(SUM(conso_mean), 2) as total_kwh
    FROM v_conso
    GROUP BY commune
    ORDER BY total_kwh DESC
    LIMIT 5
""").show()

print("\n--- REQUÊTE 2 : PERFORMANCE (IPE) MOYENNE PAR TYPE ---")
spark.sql("""
    SELECT type, ROUND(AVG(ipe), 4) as ipe_moyen
    FROM v_conso
    GROUP BY type
    ORDER BY ipe_moyen DESC
""").show()

print("\n--- REQUÊTE 3 : ANALYSE DES ANOMALIES (HORS-NORMES) ---")

spark.sql("""
    SELECT nom, commune, type, ROUND(ipe, 4) as ipe_actuel, ROUND(mediane_type, 4) as mediane_ref
    FROM v_conso
    WHERE ipe > (mediane_type * 3)
    ORDER BY ipe DESC
""").show(10)

print("\n--- REQUÊTE 4 : IMPACT TEMPÉRATURE (CORRÉLATION) ---")
spark.sql("""
    SELECT 
        CASE WHEN temperature < 5 THEN 'Froid (<5°C)'
             WHEN temperature BETWEEN 5 AND 18 THEN 'Tempéré (5-18°C)'
             ELSE 'Chaud (>18°C)' END as tranche_temp,
        ROUND(AVG(conso_mean), 2) as conso_moyenne
    FROM v_conso
    GROUP BY 1
    ORDER BY conso_moyenne DESC
""").show()

print("Analyse terminée avec succès.")
spark.stop()