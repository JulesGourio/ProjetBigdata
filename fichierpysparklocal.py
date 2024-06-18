import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Arrêter tout contexte existant
try:
    sc = SparkContext.getOrCreate()
    sc.stop()
except:
    pass

# Configuration du cluster Spark en mode local
conf = SparkConf() \
    .setAppName("Ligue1 Championship Analysis") \
    .setMaster("local[*]")  # Utilise toutes les cores disponibles sur votre machine locale

sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Lire le fichier CSV
df = spark.read.csv("Ligue1 Championship.csv", header=True, inferSchema=True)

# Afficher le schéma du DataFrame
df.printSchema()

# Exemples de transformations et actions
# Calculer le nombre de lignes
row_count = df.count()
print(f"Total Rows: {row_count}")

# Afficher les premières lignes
df.show(5)

# Calculer quelques statistiques descriptives
df.describe().show()

# Terminer le contexte Spark
sc.stop()
