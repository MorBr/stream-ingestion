import json
import time
from datetime import datetime
from collections import defaultdict, deque
import requests
from typing import Dict, List, Optional, Any
import os
from snowflake.snowpark import Session
from confluent_kafka import Consumer, Producer, KafkaError
import argparse

# Constants
ILS_EXCHANGE_RATE = 3.7  # Fallback constant multiplier
STOCK_QUOTES_TOPIC = "stock-quotes"
ENRICHED_QUOTES_TOPIC = "enriched-stock-quotes"
HOURLY_AGGREGATED_TOPIC = "hourly-aggregated-quotes"
MOVING_AVG_WINDOW_SIZE = 3


class StockQuote:
    """Stock quote data model"""

    def __init__(self, ticker: str, price: float, timestamp: int):
        self.ticker = ticker
        self.price = price
        self.timestamp = timestamp
        self.price_ils = None
        self.moving_avg = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker,
            "price": self.price,
            "timestamp": self.timestamp,
            "price_ils": self.price_ils,
            "moving_avg": self.moving_avg
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StockQuote':
        quote = cls(data["ticker"], data["price"], data["timestamp"])
        quote.price_ils = data.get("price_ils")
        quote.moving_avg = data.get("moving_avg")
        return quote

    def __str__(self) -> str:
        return (f"StockQuote(ticker={self.ticker}, price={self.price}, "
                f"timestamp={self.timestamp}, price_ils={self.price_ils}, "
                f"moving_avg={self.moving_avg})")


class HourlyAggregation:
    """Hourly aggregation data model"""

    def __init__(self, ticker: str, hour_timestamp: int = 0, quote_count: int = 0):
        self.ticker = ticker
        self.hour_timestamp = hour_timestamp
        self.quote_count = quote_count

    def increment_count(self):
        self.quote_count += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker,
            "hour_timestamp": self.hour_timestamp,
            "quote_count": self.quote_count
        }

    def __str__(self) -> str:
        return (f"HourlyAggregation(ticker={self.ticker}, "
                f"hour_timestamp={self.hour_timestamp}, quote_count={self.quote_count})")


class StockStreamProcessor:
    """Main stream processor class that handles all enrichments"""

    def __init__(self, kafka_bootstrap_servers: str, snowflake_config: Optional[Dict[str, str]] = None):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.snowflake_config = snowflake_config

        # Data structures for state management
        self.price_history = defaultdict(lambda: deque(maxlen=MOVING_AVG_WINDOW_SIZE))
        self.processed_timestamps = {}  # For deduplication
        self.hourly_aggregations = defaultdict(lambda: HourlyAggregation(""))

        # Initialize Kafka producer
        self.producer = Producer({
            'bootstrap.servers': kafka_bootstrap_servers
        })

        # Initialize Snowflake connection if config provided
        self.snowflake_session = None
        if snowflake_config:
            self.init_snowflake_connection()

    def init_snowflake_connection(self):
        """Initialize Snowflake connection if config is provided"""
        try:
            self.snowflake_session = Session.builder.configs(self.snowflake_config).create()
            print("Connected to Snowflake")
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            self.snowflake_session = None

    def get_exchange_rate(self) -> float:
        """Get USD to ILS exchange rate from external API"""
        try:
            response = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=5)
            data = response.json()
            return data["rates"]["ILS"]
        except Exception as e:
            print(f"Error fetching exchange rate: {e}")
            return ILS_EXCHANGE_RATE

    def calculate_moving_average(self, ticker: str, price: float) -> Optional[float]:
        """Calculate moving average of the last N entries for a ticker"""
        # Add current price to history
        self.price_history[ticker].append(price)

        # Calculate moving average if we have enough data points
        if len(self.price_history[ticker]) >= MOVING_AVG_WINDOW_SIZE:
            sum_prices = sum(self.price_history[ticker])
            # Format to 1 decimal place, same as example
            return round(sum_prices / MOVING_AVG_WINDOW_SIZE, 1)
        return None

    def update_hourly_aggregation(self, ticker: str, timestamp: int):
        """Update hourly aggregation for the ticker"""
        # Convert to hour timestamp (floor to hour)
        hour_timestamp = (timestamp // 3600000) * 3600000
        key = f"{ticker}_{hour_timestamp}"

        if self.hourly_aggregations[key].ticker == "":
            self.hourly_aggregations[key].ticker = ticker
            self.hourly_aggregations[key].hour_timestamp = hour_timestamp

        self.hourly_aggregations[key].increment_count()

    def enrich_quote(self, quote: StockQuote) -> StockQuote:
        """Apply all enrichments to a stock quote"""
        # 1. Currency conversion
        exchange_rate = self.get_exchange_rate()
        quote.price_ils = round(quote.price * exchange_rate, 2)

        # 2. Moving average calculation
        quote.moving_avg = self.calculate_moving_average(quote.ticker, quote.price)

        # 3. Update hourly aggregation
        self.update_hourly_aggregation(quote.ticker, quote.timestamp)

        return quote

    def process_message(self, message: Dict[str, Any]) -> Optional[StockQuote]:
        """Process an incoming message with deduplication"""
        try:
            # Convert message to StockQuote object
            ticker = message.get("ticker")
            price = float(message.get("price"))
            timestamp = int(message.get("timestamp"))

            # Skip if no ticker or price
            if not ticker or price <= 0:
                return None

            # Deduplication - check if we've seen this ticker with a newer timestamp
            if ticker in self.processed_timestamps and self.processed_timestamps[ticker] >= timestamp:
                print(f"Skipping duplicate for {ticker} (ts: {timestamp})")
                return None

            # Update latest processed timestamp for this ticker
            self.processed_timestamps[ticker] = timestamp

            # Create and enrich the quote
            quote = StockQuote(ticker, price, timestamp)
            return self.enrich_quote(quote)

        except Exception as e:
            print(f"Error processing message: {e}")
            return None

    def delivery_report(self, err, msg):
        """Kafka delivery callback"""
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            pass
            # Uncomment for verbose logging
            # print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_to_kafka(self, topic: str, key: str, value: Dict[str, Any]):
        """Send a message to a Kafka topic"""
        try:
            self.producer.produce(
                topic,
                key=key.encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)  # Trigger delivery reports
        except Exception as e:
            print(f"Error sending to Kafka: {e}")

    def save_to_snowflake(self, table_name: str, data: List[Dict[str, Any]]):
        """Save data to Snowflake table"""
        if not self.snowflake_session:
            print("Snowflake connection not available")
            return False

        try:
            # Convert to DataFrame and write to Snowflake
            df = self.snowflake_session.create_dataframe(data)
            df.write.mode("append").save_as_table(table_name)
            print(f"Saved {len(data)} records to Snowflake table {table_name}")
            return True
        except Exception as e:
            print(f"Error saving to Snowflake: {e}")
            return False


    def flush_hourly_aggregations(self):
        """Flush hourly aggregations to Kafka and/or Snowflake"""
        if not self.hourly_aggregations:
            return

        aggs_data = []
        for key, agg in self.hourly_aggregations.items():
            # Send to Kafka
            self.send_to_kafka(
                HOURLY_AGGREGATED_TOPIC,
                f"{agg.ticker}_{agg.hour_timestamp}",
                agg.to_dict()
            )

            # Add to batch for Snowflake
            aggs_data.append(agg.to_dict())

        # Save to Snowflake if connection available
        if self.snowflake_session and aggs_data:
            self.save_to_snowflake("HOURLY_AGGREGATED_QUOTES", aggs_data)

        # Clear aggregations
        self.hourly_aggregations.clear()

    def start_processing(self):
        """Start consuming messages from Kafka"""
        consumer = Consumer({
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'group.id': 'stock-stream-processor',
            'auto.offset.reset': 'earliest'
        })

        consumer.subscribe([STOCK_QUOTES_TOPIC])

        last_flush_time = datetime.timestamp(datetime.now())

        try:
            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f"Reached end of partition {msg.partition()}")
                    else:
                        print(f"Error: {msg.error()}")
                    continue

                # Process the message
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    quote = self.process_message(data)

                    if quote:
                        # Send enriched quote to output topic
                        self.send_to_kafka(
                            ENRICHED_QUOTES_TOPIC,
                            quote.ticker,
                            quote.to_dict()
                        )

                        # Save to Snowflake if connection available
                        if self.snowflake_session:
                            self.save_to_snowflake("ENRICHED_STOCK_QUOTES", [quote.to_dict()])

                except Exception as e:
                    print(f"Error in message processing loop: {e}")

                # Flush hourly aggregations periodically (every 1 hour)
                current_time = datetime.timestamp(datetime.now())
                if current_time - last_flush_time > 3600:
                    self.flush_hourly_aggregations()
                    last_flush_time = current_time

        except KeyboardInterrupt:
            print("Stopping consumer")
        finally:
            # Flush aggregations one last time
            self.flush_hourly_aggregations()

            # Close consumer and producer
            consumer.close()
            self.producer.flush()


class StockDataSimulator:
    """Simulates stock data for testing the stream processor"""

    def __init__(self, kafka_bootstrap_servers: str, speed: str = "MAX"):
        self.producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})
        self.speed = speed.upper()
        self.tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA", "NFLX", "WIX"]

    def delivery_report(self, err, msg):
        """Callback for Kafka message delivery"""
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            pass
            # Uncomment for verbose logging
            # print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def generate_stock_data(self, count: int = 100):
        """Generate and send random stock data to Kafka"""
        import random

        print(f"Generating {count} stock quotes with speed {self.speed}")
        start_time = datetime.timestamp(datetime.now())

        for i in range(1, count + 1):
            # Generate random stock data
            ticker = random.choice(self.tickers)
            # Base price between 100 and 300
            price = round(random.uniform(100, 300), 2)
            timestamp = datetime.timestamp(datetime.now())

            # Create message
            message = {
                "ticker": ticker,
                "price": price,
                "timestamp": timestamp
            }

            # Add delay based on speed
            if self.speed != "MAX":
                if self.speed == "SLOW":
                    time.sleep(0.001)  # 1000/second
                elif self.speed == "SLOOW":
                    time.sleep(0.01)  # 100/second
                elif self.speed == "SLOOOW":
                    time.sleep(0.1)  # 10/second
                elif self.speed == "SLOOOOW":
                    time.sleep(1.0)  # 1/second
                print(f"{i} ", end="", flush=True)

            # Send to Kafka
            self.producer.produce(
                STOCK_QUOTES_TOPIC,
                key=ticker.encode('utf-8'),
                value=json.dumps(message).encode('utf-8'),
                callback=self.delivery_report
            )

            # Trigger callbacks to catch any errors
            self.producer.poll(0)

        # Ensure all messages are sent
        self.producer.flush()

        elapsed = datetime.timestamp(datetime.now()) - start_time
        print(f"\nRows Sent: {count}")
        print(f"Time to Send: {elapsed:.3f} seconds")
        print(f"Rate: {count / elapsed:.2f} messages/second")


def load_config(profile_path: str = "./snowflake.properties") -> Dict[str, str]:
    """Load configuration from properties file"""
    config = {}
    try:
        if os.path.exists(profile_path):
            with open(profile_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip()
        else:
            print(f"Config file not found: {profile_path}")
    except Exception as e:
        print(f"Error loading config: {e}")

    return config


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Stock Stream Processor')
    parser.add_argument('--mode', choices=['process', 'simulate'], default='process',
                        help='Mode: process (process data) or simulate (generate data)')
    parser.add_argument('--speed', default='MAX',
                        help='Simulation speed: MAX, SLOW, SLOOW, SLOOOW, SLOOOOW')
    parser.add_argument('--count', type=int, default=1000,
                        help='Number of messages to generate in simulation mode')
    parser.add_argument('--kafka', default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--config', default='./snowflake.properties',
                        help='Path to Snowflake configuration file')

    args = parser.parse_args()

    # Load Snowflake configuration
    snowflake_config = load_config(args.config)

    if args.mode == 'simulate':
        # Run simulator to generate data
        simulator = StockDataSimulator(args.kafka, args.speed)
        simulator.generate_stock_data(args.count)
    else:
        # Run processor to process data
        processor = StockStreamProcessor(args.kafka, snowflake_config)
        print("Starting stock stream processor...")
        processor.start_processing()
