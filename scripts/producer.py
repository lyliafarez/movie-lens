#imports
from kafka import KafkaProducer
import json
import time
from datetime import datetime
from pyspark.sql.functions import year, month, col
import random


#Connexion à Kafka
TOPIC_NAME = 'movie_rating'
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#initialisation de la session spark
"""spark = SparkSession.builder \
        .appName("RatingsStreamProcessor") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()"""

#Générer des notes
def simulate_ratings():
    user_id = random.choice(list(range(138000)))
    movie_id = random.choice(list(range(26744)))
    rating = round(random.uniform(0.5, 5.0), 1)
    timestamp = int(time.time())
    return {
        'userId' : user_id,
        'movieId' : movie_id,
        'rating' : rating,
        'timestamp' : timestamp
    }

#Fonction pour envoyer les ratings à Kafka
def send_ratings():
    try:
        print(f"Ratings en cours d'envoi sur le topic suivant : {TOPIC_NAME}")
        while True:
            rating = simulate_ratings()
            producer.send(TOPIC_NAME, value=rating)
            print(f"Rating envoyé: {rating}")
            # Convert to DataFrame with timestamp parsing and store in hdfs
            """rating_df = spark.createDataFrame([rating])
            
            # Add partitioning columns (year and month from timestamp)
            rating_df = rating_df.withColumn("timestamp_col", 
                                            col("timestamp").cast("timestamp"))
            rating_df = rating_df.withColumn("rating_year", 
                                            year("timestamp_col"))
            rating_df = rating_df.withColumn("rating_month", 
                                            month("timestamp_col"))
            
            # Save with partitioning - same logic as your example
            (rating_df.repartition(100, "rating_year", "rating_month")
                .write
                .partitionBy("rating_year", "rating_month")
                .parquet("hdfs://namenode:9000/movie-lens/processed/ratings",
                        mode="append"))  # Using append instead of overwrite for streaming"""
            time.sleep(2)
    except Exception as e:
        print(f"Erreur {e}")
        # Prepare error data with additional context
        """error_data = {
            "error_timestamp": datetime.now().isoformat(),
            "error_message": str(e),
            "failed_rating": str(rating) if 'rating' in locals() else "N/A",
            "stack_trace": traceback.format_exc()
        }
        
        # Save error with similar partitioning by error date
        error_df = spark.createDataFrame([error_data])
        error_df = error_df.withColumn("error_year", 
                                        year(col("error_timestamp")))
        error_df = error_df.withColumn("error_month", 
                                        month(col("error_timestamp")))
        
        (error_df.repartition(10, "error_year", "error_month")
            .write
            .partitionBy("error_year", "error_month")
            .parquet("hdfs://namenode:9000/movie-lens/processed/errors",
                    mode="append"))"""
    finally:
        producer.close()

send_ratings()

