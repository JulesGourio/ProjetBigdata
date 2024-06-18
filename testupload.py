
# Créez une session Spark

# Importez les données
#df = spark.read.csv("Ligue1 Championship.csv", header=True, inferSchema=True)

#!pip install plotly
import plotly.express as px
import pandas as pd  # Utilisé pour manipuler les données CSV
#df = spark.read.csv("Ligue1 Championship.csv", header=True, inferSchema=True)
import pandas as pd

# Chargement des données depuis le fichier CSV
df = pd.read_csv('Ligue1 Championship.csv')

# Renommer les colonnes pour standardiser les noms
df.rename(columns={
    'Home Team': 'Team',
    'Away Team': 'Opponent',
    'Home Team Goals': 'Goals For',
    'Away Team Goals': 'Goals Against'
}, inplace=True)

# Préparer les données pour avoir une ligne par équipe avec les buts marqués et encaissés
goals_scored_home = df[['Team', 'Opponent', 'Goals For', 'Goals Against']].copy()
goals_scored_home['Home Match'] = True  # Indicateur de match à domicile

goals_scored_away = df[['Opponent', 'Team', 'Goals Against', 'Goals For']].copy()
goals_scored_away['Home Match'] = False  # Indicateur de match à l'extérieur

# Concaténer les deux dataframes
transformed_df = pd.concat([goals_scored_home, goals_scored_away], ignore_index=True)

# Calculer le nombre de matchs joués par chaque équipe
matches_played = transformed_df.groupby('Team').size().reset_index(name='Matches Played')

# Calculer le nombre total de buts marqués par chaque équipe
goals_scored = transformed_df.groupby('Team')['Goals For'].sum().reset_index(name='Total Goals Scored')

# Fusionner les données de matchs joués et de buts marqués
team_stats = pd.merge(matches_played, goals_scored, on='Team')

# Calculer la moyenne de buts marqués par match pour chaque équipe
team_stats['Goals Scored Avg'] = team_stats['Total Goals Scored'] / team_stats['Matches Played']

# Calculer le nombre total de buts encaissés par chaque équipe
goals_against = transformed_df.groupby('Team')['Goals Against'].sum().reset_index(name='Total Goals Against')

# Fusionner les données de buts encaissés avec team_stats
team_stats = pd.merge(team_stats, goals_against, on='Team')

# Remplacer les valeurs NaN dans Total Goals Against par 0
team_stats['Total Goals Against'].fillna(0, inplace=True)

# Calculer le nombre total de défaites par équipe
defeats = transformed_df.groupby('Team').apply(lambda x: (x['Goals For'] < x['Goals Against']).sum()).reset_index(name='Total Defeats')

# Fusionner les données de matchs joués, buts marqués, et défaites
team_stats = pd.merge(team_stats, defeats, on='Team')

# Calculer le nombre total de victoires par équipe
team_stats['Total Wins'] = team_stats['Matches Played'] - team_stats['Total Defeats']

# Calculer le ratio de victoires/défaites
team_stats['Win Ratio'] = team_stats['Total Wins'] / (team_stats['Total Defeats'] + team_stats['Total Wins'])

# Trouver l'équipe avec le meilleur ratio victoires/défaites
best_win_loss_ratio_team = team_stats.loc[team_stats['Win/Loss Ratio'].idxmax()]

print("\nÉquipe avec le meilleur ratio victoires/défaites :")
print(best_win_loss_ratio_team[['Team', 'Win/Loss Ratio', 'Matches Played', 'Total Wins', 'Total Goals Scored', 'Total Goals Against']])

# Générer le csv
team_stats.to_csv('team_stats.csv', index=False)

