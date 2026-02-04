from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType
from datetime import datetime
import os
import sys

DATA_DIR = "/data_ecf"
OUTPUT_DIR = "/output"

CONSO_RAW_PATH = os.path.join(DATA_DIR, "consommations_raw.csv")
BATIMENTS_PATH = os.path.join(DATA_DIR, "batiments.csv")


def create_spark_session():
    return SparkSession.builder \
        .appName("ECF2 - Nettoyage") \
        .master("local[*]") \
        .getOrCreate()


def parse_multi_format_timestamp(timestamp_str):
    """
    UDF pour parser les timestamps multi-formats.
    Formats supportes:
    - %Y-%m-%d %H:%M:%S (ISO)
    - %d/%m/%Y %H:%M (FR)
    - %m/%d/%Y %H:%M:%S (US)
    - %Y-%m-%dT%H:%M:%S (ISO avec T)
    """
    if timestamp_str is None:
        return None

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue

    return None


def clean_conso(conso_str):
    """
    UDF pour nettoyer les valeurs numeriques.
    - Remplace la virgule par un point
    - Retourne None pour les valeurs non numeriques
    """
    if conso_str is None:
        return None

    try:
        # Remplacer virgule par point
        clean_str = conso_str.replace(",", ".")
        return float(clean_str)
    except (ValueError, AttributeError):
        return None
    

def main():
    spark = create_spark_session()
    print(f"Spark version: {spark.version}")

    parse_timestamp_udf = F.udf(parse_multi_format_timestamp, TimestampType())
    clean_value_udf = F.udf(clean_conso, DoubleType())

    print("\n[1/6] Chargement des donnees brutes...")
    df_raw = spark.read \
        .option("header", "true") \
        .csv(CONSO_RAW_PATH)

    initial_count = df_raw.count()
    print(f"  Lignes en entree: {initial_count:,}")

    df_batiments = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(BATIMENTS_PATH)
    
    print("\n[2/6] Parsing des timestamps multi-formats...")
    df_with_timestamp = df_raw.withColumn(
        "timestamp_parsed",
        parse_timestamp_udf(F.col("timestamp"))
    )

    invalid_timestamps = df_with_timestamp.filter(F.col("timestamp_parsed").isNull()).count()
    df_with_timestamp = df_with_timestamp.filter(F.col("timestamp_parsed").isNotNull())
    print(f"  Timestamps invalides supprimes: {invalid_timestamps:,}")


    print("\n[3/6] Conversion des valeurs numeriques...")
    df_with_values = df_with_timestamp.withColumn(
        "conso_clean",
        clean_value_udf(F.col("consommation"))
    )

    invalid_values = df_with_values.filter(F.col("conso_clean").isNull()).count()
    df_with_values = df_with_values.filter(F.col("conso_clean").isNotNull())
    print(f"  Valeurs non numeriques supprimees: {invalid_values:,}")


    print("\n[4/6] Suppression des valeurs aberrantes...")
    negative_count = df_with_values.filter(F.col("conso_clean") < 0).count()
    outlier_count = df_with_values.filter(F.col("conso_clean") > 10000).count()

    df_clean = df_with_values.filter(
        (F.col("conso_clean") >= 0) & (F.col("conso_clean") <= 10000)
    )
    print(f"  Consommations negatives supprimees: {negative_count:,}")
    print(f"  Outliers (>15000) supprimes: {outlier_count:,}")


    print("\n[5/6] Deduplication...")
    before_dedup = df_clean.count()
    df_dedup = df_clean.dropDuplicates(["batiment_id", "timestamp_parsed", "type_energie"])
    after_dedup = df_dedup.count()
    duplicates_removed = before_dedup - after_dedup
    print(f"  Doublons supprimes: {duplicates_removed:,}")



    print("\n[6/6] Agregation horaire et sauvegarde...")

  
    df_enriched = df_dedup.join(
        df_batiments.select("batiment_id", "nom", "commune", "type"),
        on="batiment_id",
        how="left"
    )


    df_with_time = df_enriched.withColumn(
        "date", F.to_date(F.col("timestamp_parsed"))
    ).withColumn(
        "hour", F.hour(F.col("timestamp_parsed"))
    ).withColumn(
        "year", F.year(F.col("timestamp_parsed"))
    ).withColumn(
        "month", F.month(F.col("timestamp_parsed"))
    )


    df_hourly = df_with_time.groupBy(
        "batiment_id", "type_energie", "unite", "date", "hour", "year", "month"
    ).agg(
        F.round(F.mean("conso_clean"), 2).alias("conso_mean"),
        F.round(F.min("conso_clean"), 2).alias("conso_min"),
        F.round(F.max("conso_clean"), 2).alias("conso_max"),
        F.count("*").alias("measurement_count")
    )


    path_final = os.path.join(OUTPUT_DIR, "consommations_clean_partitionne")

    print(f"\n[Sauvegarde] Écriture du dataset partitionné par DATE et TYPE_ENERGIE...")


    df_hourly.write \
    .mode("overwrite") \
    .partitionBy("date", "type_energie") \
    .parquet(path_final)

    print(f"Sauvegarde terminée dans : {path_final}")

    df_final = df_hourly 
    final_count = df_final.count()


    print("RAPPORT DE NETTOYAGE")
    print(f"Lignes en entree:              {initial_count:>12,}")
    print(f"Timestamps invalides:          {invalid_timestamps:>12,}")
    print(f"Valeurs non numeriques:        {invalid_values:>12,}")
    print(f"Valeurs negatives:             {negative_count:>12,}")
    print(f"Outliers (>10000):              {outlier_count:>12,}")
    print(f"Doublons:                      {duplicates_removed:>12,}")
    total_removed = invalid_timestamps + invalid_values + negative_count + outlier_count + duplicates_removed
    print(f"Total lignes supprimees:       {total_removed:>12,}")
    print(f"Lignes apres agregation:       {final_count:>12,}")
    print(f"\nFichiers Parquet sauvegardes dans: {OUTPUT_DIR}")


    print("\nApercu des donnees nettoyees:")
    df_final.show(10)



    print("\nStatistiques par type d'énergie:")
    df_final.groupBy("type_energie") \
        .agg(
            F.count("*").alias("records"),
            F.round(F.mean("conso_mean"), 2).alias("avg_conso"),
            F.round(F.min("conso_min"), 2).alias("min_conso"),
            F.round(F.max("conso_max"), 2).alias("max_conso")
        ) \
        .orderBy("type_energie") \
        .show()


    spark.stop()


if __name__ == "__main__":
    main()