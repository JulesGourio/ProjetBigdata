#!/bin/bash

# Définir les variables d'environnement
export JAVA_HOME=/home/jdk1.8.0_202
export SPARK_HOME=/home/spark-2.4.3-bin-hadoop2.7

# Supprimer les fichiers temporaires Hadoop
rm -rf /tmp/hadoop*

# Supprimer les fichiers temporaires Hadoop sur slave1
ssh mathilde2@slave1 'rm -rf /home/hadoop/hdfs/datanode/*; rm -rf /tmp/hadoop*'

# Formater le NameNode
hdfs namenode -format

# Démarrer HDFS
start-dfs.sh

# Démarrer le maître Spark
start-master.sh

# Démarrer les esclaves Spark sur slave1
ssh mathilde2@slave1 " export JAVA_HOME=$JAVA_HOME ; $SPARK_HOME/sbin/start-slave.sh spark://master:7077"