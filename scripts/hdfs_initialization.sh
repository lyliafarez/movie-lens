#!/bin/bash

# Script: hdfs_movielens_setup.sh
# Description: Automates the process of copying MovieLens data to HDFS
# Usage: ./hdfs_movielens_setup.sh

# -------------------------------
# Étape 1: Copie des fichiers depuis Windows vers le conteneur
# Step 1: Copy files from Windows to container
# -------------------------------
echo "1. COPYING FILES TO CONTAINER..."
echo "1. COPIE DES FICHIERS VERS LE CONTENEUR..."

# Naviguer vers le répertoire des données (à adapter selon votre machine)
# Navigate to data directory (adjust for your machine)
cd "C:\Users\OneDrive\Documents\data" || {
    echo "Error: Data directory not found!"
    echo "Erreur: Répertoire des données introuvable!"
    exit 1
}

# Copier les fichiers vers le conteneur
# Copy files to container
docker cp movie.csv bigdata-container:/tmp/ && \
docker cp rating.csv bigdata-container:/tmp/ || {
    echo "Error: File copy failed!"
    echo "Erreur: Échec de la copie des fichiers!"
    exit 1
}

# -------------------------------
# Étape 2: Création de l'architecture HDFS
# Step 2: Create HDFS architecture
# -------------------------------
echo "2. CREATING HDFS DIRECTORY STRUCTURE..."
echo "2. CRÉATION DE L'ARCHITECTURE HDFS..."

# Exécuter les commandes HDFS dans le conteneur
# Execute HDFS commands in container
docker exec -i bigdata-container bash << 'EOF'
#!/bin/bash

# Créer les répertoires principaux
# Create main directories
hdfs dfs -mkdir -p /movie-lens/rawdata && \
hdfs dfs -mkdir -p /movie-lens/processed/movies && \
hdfs dfs -mkdir -p /movie-lens/processed/ratings && \
hdfs dfs -mkdir -p /movie-lens/archive/movies && \
hdfs dfs -mkdir -p /movie-lens/archive/ratings || {
    echo "Error: HDFS directory creation failed!"
    echo "Erreur: Échec de création des répertoires HDFS!"
    exit 1
}

# Vérifier la structure créée
# Verify created structure
echo "HDFS Directory Structure:"
echo "Structure des répertoires HDFS:"
hdfs dfs -ls -R /movie-lens

# -------------------------------
# Étape 3: Chargement des données dans HDFS
# Step 3: Load data into HDFS
# -------------------------------
echo "3. LOADING DATA TO HDFS..."
echo "3. CHARGEMENT DES DONNÉES DANS HDFS..."

hdfs dfs -put -f /tmp/movie.csv /movie-lens/rawdata/ && \
hdfs dfs -put -f /tmp/rating.csv /movie-lens/rawdata/ || {
    echo "Error: File upload to HDFS failed!"
    echo "Erreur: Échec du téléversement vers HDFS!"
    exit 1
}

# Vérifier les fichiers téléversés
# Verify uploaded files
echo "Uploaded Files:"
echo "Fichiers téléversés:"
hdfs dfs -ls /movie-lens/rawdata

# Vérification finale
# Final verification
echo "HDFS Setup Completed Successfully!"
echo "Configuration HDFS terminée avec succès!"

EOF

# -------------------------------
# Fin du script
# End of script
# -------------------------------
echo "Script executed successfully!"
echo "Script exécuté avec succès!"
exit 0