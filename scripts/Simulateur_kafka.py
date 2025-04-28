import time
import random
from kafka import KafkaProducer
import json
import pandas as pd

# Configuration de Kafka
KAFKA_BROKER = 'localhost:9092'  # Remplacez par l'adresse de votre broker Kafka
TOPIC_NAME = 'movielens_ratings'

# Création du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Chargement des données MovieLens
ratings_df = pd.read_csv('ratings.csv')
movies_df = pd.read_csv('movies.csv')

# Extraction des utilisateurs et films uniques
users = ratings_df['userId'].unique().tolist()
movies = movies_df['movieId'].unique().tolist()

# Fonction de simulation des notes
def generate_rating():
    user_id = random.choice(users)
    movie_id = random.choice(movies)
    rating = round(random.uniform(0.5, 5.0), 1)  # Note entre 0.5 et 5.0
    timestamp = int(time.time())  # Timestamp actuel
    return {
        'userId': user_id,
        'movieId': movie_id,
        'rating': rating,
        'timestamp': timestamp
    }

# Envoi des messages à Kafka
try:
    print(f"Envoi des messages à Kafka sur le topic '{TOPIC_NAME}'...")
    while True:
        message = generate_rating()
        producer.send(TOPIC_NAME, value=message)
        print(f"Message envoyé: {message}")
        time.sleep(random.uniform(2.0, 5.0))  # Envoi toutes les 2 à 5 secondes
except KeyboardInterrupt:
    print("Simulation arrêtée.")
except Exception as e:
    print(f"Erreur lors de l'envoi: {e}")
finally:
    producer.close()