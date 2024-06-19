from flask import Flask, request, render_template, redirect, url_for
import os
import subprocess
import pandas as pd

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads/'

master = 'mathilde1@192.168.122.55'

# Fonction utilitaire pour lister les fichiers dans un répertoire avec un filtre sur les extensions
def list_files(directory, extensions=None):
    if extensions is None:
        extensions = []
    return [f for f in os.listdir(directory) if any(f.endswith(ext) for ext in extensions)]

# Route pour la page d'accueil
@app.route('/')
def index():
    data_files = list_files(os.path.join(app.config['UPLOAD_FOLDER'], 'data'))
    program_files = list_files(os.path.join(app.config['UPLOAD_FOLDER'], 'programs'), extensions=['.py', '.jar'])
    return render_template('index.html', data_files=data_files, program_files=program_files, tables=None)

# Route pour l'upload des données
@app.route('/upload_data', methods=['POST'])
def upload_data():
    file = request.files['file']
    hdfs_path = request.form['hdfs_path']
    file.save(os.path.join(app.config['UPLOAD_FOLDER'], 'data', file.filename))
    subprocess.run(['hdfs', 'dfs', '-put', os.path.join(app.config['UPLOAD_FOLDER'], 'data', file.filename), hdfs_path])
    return redirect(url_for('index'))

# Route pour l'upload des programmes (scripts PySpark et fichiers .jar)
@app.route('/upload_program', methods=['POST'])
def upload_program():
    file = request.files['file']
    file.save(os.path.join(app.config['UPLOAD_FOLDER'], 'programs', file.filename))
    return redirect(url_for('index'))

# Route pour exécuter le job (script PySpark ou .jar)
@app.route('/execute_job', methods=['POST'])
def execute_job():
    program = request.form['program']
    input_path = request.form['input_path']
    output_path = request.form['output_path']
    main_class = request.form.get('main_class', '')
    program_path = os.path.join(app.config['UPLOAD_FOLDER'], 'programs', program)
    output_local_path = "/home/output/out.txt"

    if program.endswith('.py'):
        # Exécuter le job Spark avec un script PySpark sur le nœud master
        subprocess.run(['ssh', master, 'spark-submit', program_path, input_path, output_path])
    elif program.endswith('.jar'):
        # Exécuter les scripts nécessaires pour les fichiers .jar sur le nœud master
        subprocess.run(['ssh', master, 'cd /home/sujet-tp-scale/'])

        subprocess.run(['ssh', master, 'source', 'comp.sh'])
        subprocess.run(['ssh', master, 'source', 'generate.sh', 'filesample.txt', '20'])
        subprocess.run(['ssh', master, 'source', '../start1.sh'])
        subprocess.run(['ssh', master, 'source', 'copy.sh'])
        subprocess.run(['ssh', master, 'source', 'run.sh'])
        #subprocess.run(['ssh', master, 'spark-submit', '--class', main_class, program_path, input_path, output_path])
        subprocess.run(['ssh', master, 'hdfs', 'dfs', '-getmerge', output_path + '/*', output_local_path])
        subprocess.run(['ssh', master, 'source', 'stop.sh'])

    # Récupérer les résultats depuis HDFS vers le système de fichiers local sur le nœud master

    # Charger les résultats dans un DataFrame Pandas (optionnel)
    #df = pd.read_csv(output_local_path)

    # Traiter les résultats ou les transmettre à la vue selon vos besoins

    return redirect(url_for('index'))

# Point d'entrée pour l'exécution de l'application Flask
if __name__ == '__main__':
    app.run(debug=True)
