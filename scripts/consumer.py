#!/usr/bin/env python3
"""
Spark Kafka Consumer for Movie Recommendations
"""

import sys
import time
import signal
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType
from pyspark.ml.recommendation import ALSModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def initialize_spark():
    """Initialize and return Spark session with configurations"""
    try:
        spark = SparkSession.builder \
            .appName("MovieRecommendationConsumer") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                   "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .config("spark.mongodb.output.uri", 
                   "mongodb://host.docker.internal:27017/movie_lens.recommendations") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
        
        logger.info("Spark session initialized successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {str(e)}")
        sys.exit(1)

def load_model(spark, model_path):
    """Load and return the ALS recommendation model"""
    try:
        logger.info(f"Attempting to load model from: {model_path}")
        model = ALSModel.load(model_path)
        logger.info("ALS model loaded successfully")
        return model
    except Exception as e:
        logger.error(f"Failed to load ALS model: {str(e)}")
        spark.stop()
        sys.exit(1)

def generate_recommendations(model, user_ids_df):
    """Generate movie recommendations for users"""
    try:
        # Get distinct users
        distinct_users = user_ids_df.select("userId").distinct()
        logger.info(f"Generating recommendations for {distinct_users.count()} users")
        
        # Generate recommendations
        recommendations = model.recommendForUserSubset(distinct_users, 5)
        
        # Explode recommendations into rows
        exploded_recs = recommendations.select(
            "userId",
            F.explode("recommendations").alias("recommendation")
        ).select(
            "userId",
            F.col("recommendation.movieId").alias("movieId"),
            F.col("recommendation.rating").alias("rating")
        )
        
        logger.info(f"Generated {exploded_recs.count()} total recommendations")
        return exploded_recs
    except Exception as e:
        logger.error(f"Error generating recommendations: {str(e)}")
        raise

def process_batch(batch_df, batch_id):
    """Process each batch of streaming data"""
    if not batch_df.isEmpty():
        try:
            logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")
            
            # Generate recommendations
            recommendations_df = generate_recommendations(model, batch_df)
            
            # Add metadata
            result_df = recommendations_df.withColumn("processing_time", F.current_timestamp()) \
                                         .withColumn("batch_id", F.lit(batch_id))
            
            # Write to MongoDB
            result_df.write \
                .format("mongo") \
                .mode("append") \
                .option("database", "movie_lens") \
                .option("collection", "recommendations") \
                .save()
            
            logger.info(f"Successfully saved {result_df.count()} recommendations from batch {batch_id}")
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {str(e)}")

def handle_shutdown(signum, frame):
    """Gracefully shutdown the streaming application"""
    logger.info("Shutting down gracefully...")
    if 'query' in globals() and query.isActive:
        query.stop()
    if 'spark' in globals():
        spark.stop()
    sys.exit(0)

def main():
    """Main execution function"""
    # Initialize Spark and model
    global spark, model, query
    spark = initialize_spark()
    model = load_model(spark, "hdfs://namenode:9000/movie-lens/models/als_model")
    
    # Define Kafka message schema
    rating_schema = StructType([
        StructField("userId", IntegerType()),
        StructField("movieId", IntegerType()),
        StructField("rating", FloatType()),
        StructField("timestamp", TimestampType())
    ])
    
    # Set up Kafka source
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "namenode:9092") \
        .option("subscribe", "movie_rating") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "1000") \
        .load()
    
    # Parse JSON data
    processed_df = df.selectExpr("CAST(value AS STRING)") \
                    .select(F.from_json(F.col("value"), rating_schema).alias("data")) \
                    .select("data.*")
    
    # Start streaming query
    query = processed_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoint_movies") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    # Set up shutdown handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    # Monitoring loop
    try:
        while query.isActive:
            progress = query.lastProgress
            if progress:
                logger.info(
                    f"Progress: Batch {progress['batchId']} | "
                    f"Input: {progress['numInputRows']} rows | "
                    f"Rate: {progress['processedRowsPerSecond']:.1f} rows/sec"
                )
            time.sleep(5)
    except Exception as e:
        logger.error(f"Streaming query failed: {str(e)}")
        handle_shutdown(None, None)

if __name__ == "__main__":
    logger.info("Starting Movie Recommendation Consumer")
    main()