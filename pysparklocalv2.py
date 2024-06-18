from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as spark_sum

# Créer une session Spark
spark = SparkSession.builder.appName("Ligue1 Championship Analysis").getOrCreate()

# Charger les données CSV
df = spark.read.csv("hdfs://master:9000/user/hadoop/Ligue1 Championship.csv", header=True, inferSchema=True)

# Renommer les colonnes
df = df.withColumnRenamed('Home Team', 'Team') \
       .withColumnRenamed('Away Team', 'Opponent') \
       .withColumnRenamed('Home Team Goals', 'Goals For') \
       .withColumnRenamed('Away Team Goals', 'Goals Against')

# Préparer les données pour avoir une ligne par équipe avec les buts marqués et encaissés
goals_scored_home = df.select(col('Team'), col('Opponent'), col('Goals For'), col('Goals Against')) \
                      .withColumn('Home Match', when(col('Team').isNotNull(), True))

goals_scored_away = df.select(col('Opponent').alias('Team'), col('Team').alias('Opponent'), col('Goals Against').alias('Goals For'), col('Goals For').alias('Goals Against')) \
                      .withColumn('Home Match', when(col('Team').isNotNull(), False))

# Concaténer les deux dataframes
transformed_df = goals_scored_home.union(goals_scored_away)

# Calculer le nombre de matchs joués par chaque équipe
matches_played = transformed_df.groupBy('Team').agg(count('Team').alias('Matches Played'))

# Calculer le nombre total de buts marqués par chaque équipe
goals_scored = transformed_df.groupBy('Team').agg(spark_sum('Goals For').alias('Total Goals Scored'))

# Fusionner les données de matchs joués et de buts marqués
team_stats = matches_played.join(goals_scored, on='Team')

# Calculer la moyenne de buts marqués par match pour chaque équipe
team_stats = team_stats.withColumn('Goals Scored Avg', col('Total Goals Scored') / col('Matches Played'))

# Calculer le nombre total de buts encaissés par chaque équipe
goals_against = transformed_df.groupBy('Team').agg(spark_sum('Goals Against').alias('Total Goals Against'))

# Fusionner les données de buts encaissés avec team_stats
team_stats = team_stats.join(goals_against, on='Team')

# Calculer le nombre total de défaites par équipe
defeats = transformed_df.groupBy('Team').agg(sum(when(col('Goals For') < col('Goals Against'), 1).otherwise(0)).alias('Total Defeats'))

# Fusionner les données de matchs joués, buts marqués, et défaites
team_stats = team_stats.join(defeats, on='Team')

# Calculer le nombre total de victoires par équipe
team_stats = team_stats.withColumn('Total Wins', col('Matches Played') - col('Total Defeats'))

# Calculer le ratio de victoires/défaites
team_stats = team_stats.withColumn('Win/Loss Ratio', col('Total Wins') / col('Total Defeats'))

# Trouver l'équipe avec le meilleur ratio victoires/défaites
best_win_loss_ratio_team = team_stats.orderBy(col('Win/Loss Ratio').desc()).first()

print("\nÉquipe avec le meilleur ratio victoires/défaites :")
print(best_win_loss_ratio_team)

# Sauvegarder le dataframe en CSV
team_stats.coalesce(1).write.csv('hdfs://master:9000/user/hadoop/team_stats.csv', header=True)

# Stopper la session Spark
spark.stop()
