#!/usr/bin/env python3
"""
Kafka Producer for Simulating Movie Ratings
"""

import json
import sys
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class RatingProducer:
    def __init__(self, bootstrap_servers='kafka:9092', topic='movie_rating'):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.initialize_producer()

    def initialize_producer(self):
        """Initialize Kafka producer with retry logic"""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                logger.info("Successfully connected to Kafka")
                return
            except KafkaError as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} - Connection failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to Kafka after multiple attempts")
                    raise

    def generate_rating(self):
        """Generate a random movie rating"""
        return {
            'userId': random.randint(1,1000),
            'movieId': random.randint(1, 26744),
            'rating': round(random.uniform(0.5, 5.0), 1),
            'timestamp': int(time.time())
        }

    def send_rating(self, rating):
        """Send a rating to Kafka with error handling"""
        try:
            future = self.producer.send(
                topic=self.topic,
                value=rating
            )
            # Block until the message is sent
            future.get(timeout=10)
            logger.info(f"Sent rating: user={rating['userId']}, movie={rating['movieId']}, rating={rating['rating']}")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send rating: {str(e)}")
            return False

    def run_producer(self, interval=2):
        """Continuously generate and send ratings"""
        logger.info(f"Starting to produce ratings to topic: {self.topic}")
        try:
            while True:
                rating = self.generate_rating()
                if not self.send_rating(rating):
                    # Wait longer if there was an error
                    time.sleep(10)
                    continue
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
        finally:
            self.close()

    def close(self):
        """Clean up the producer"""
        if self.producer:
            self.producer.flush()
            self.producer.close(timeout=5)
            logger.info("Kafka producer closed")

def wait_for_kafka(bootstrap_servers, timeout=60):
    """Wait for Kafka to become available"""
    from kafka import KafkaConsumer
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                consumer_timeout_ms=1000
            )
            consumer.topics()
            consumer.close()
            return True
        except Exception as e:
            logger.warning(f"Waiting for Kafka... ({str(e)})")
            time.sleep(5)
    
    raise TimeoutError(f"Kafka not available at {bootstrap_servers} after {timeout} seconds")

def main():
    try:
        # Wait for Kafka to be ready
        kafka_servers = 'kafka:9092'
        wait_for_kafka(kafka_servers)
        
        # Initialize and run producer
        producer = RatingProducer(
            bootstrap_servers=kafka_servers,
            topic='movie_rating'
        )
        producer.run_producer(interval=2)
    except Exception as e:
        logger.error(f"Failed to start producer: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()