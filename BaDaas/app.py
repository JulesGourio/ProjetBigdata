from flask import Flask, request, render_template, redirect, url_for
import os
import subprocess
import pandas as pd

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads/'

# Fonction utilitaire pour lister les fichiers dans un répertoire
def list_files(directory):
    return os.listdir(directory)

# Route pour la page d'accueil
@app.route('/')
def index():
    data_files = list_files(os.path.join(app.config['UPLOAD_FOLDER'], 'data'))
    program_files = list_files(os.path.join(app.config['UPLOAD_FOLDER'], 'programs'))
    return render_template('index.html', data_files=data_files, program_files=program_files, tables=None)

# Route pour l'upload des données
@app.route('/upload_data', methods=['POST'])
def upload_data():
    file = request.files['file']
    hdfs_path = request.form['hdfs_path']
    file.save(os.path.join(app.config['UPLOAD_FOLDER'], 'data', file.filename))
    subprocess.run(['hdfs', 'dfs', '-put', os.path.join(app.config['UPLOAD_FOLDER'], 'data', file.filename), hdfs_path])
    return redirect(url_for('index'))

# Route pour l'upload des programmes (scripts PySpark)
@app.route('/upload_program', methods=['POST'])
def upload_program():
    file = request.files['file']
    file.save(os.path.join(app.config['UPLOAD_FOLDER'], 'programs', file.filename))
    return redirect(url_for('index'))

# Route pour exécuter le job (script PySpark)
@app.route('/execute_job', methods=['POST'])
def execute_job():
    program = request.form['program']
    input_path = request.form['input_path']
    output_path = request.form['output_path']
    program_path = os.path.join(app.config['UPLOAD_FOLDER'], 'programs', program)
    output_local_path = os.path.join(app.config['UPLOAD_FOLDER'], 'output', 'team_stats.csv')

    if program.endswith('.py'):
        # Exécuter le job Spark avec un script PySpark
        subprocess.run(['spark-submit', program_path, input_path, output_path])
    elif program.endswith('.jar'):
        # Exécuter le job Spark avec un fichier .jar
        subprocess.run(['spark-submit', '--class', 'MainClass', program_path, input_path, output_path])

    # Récupérer les résultats depuis HDFS vers le système de fichiers local
    subprocess.run(['hdfs', 'dfs', '-get', output_path + '/*', output_local_path])

    # Charger les résultats dans un DataFrame Pandas (optionnel)
    df = pd.read_csv(output_local_path)

    # Traiter les résultats ou les transmettre à la vue selon vos besoins

    return redirect(url_for('index'))

# Point d'entrée pour l'exécution de l'application Flask
if __name__ == '__main__':
    app.run(debug=True)
