# Stock Stream Processor

## Overview

Stock Stream Processor is a real-time data processing system for financial market data. It consumes stock quotes from Kafka topics, enriches them with additional information (currency conversion, moving averages), and produces aggregated metrics for analytics purposes.

## Architecture

The system consists of two main components:

1. **Stock Stream Processor**: Consumes raw stock quotes, performs enrichments, and publishes enhanced data to output topics.
2. **Stock Data Simulator**: Generates synthetic stock data for testing or demonstration purposes.

### Data Flow

```
                      ┌─────────────────────┐
                      │ Exchange Rate API   │
                      └─────────────────────┘
                                ▲
                                │
┌─────────────┐      ┌─────────┴─────────┐      ┌─────────────┐      ┌─────────────┐
│ Raw Stock   │      │ Stock Stream      │      │ Enriched    │      │             │
│ Quotes      ├─────►│ Processor         ├─────►│ Quotes      ├─────►│             │
│ Topic       │      │                   │      │ Topic       │      │ Snowflake   │
└─────────────┘      └───────────────────┘      └─────────────┘      │ Data        │
                                │                                    │ Warehouse   │
                                ▼                                    │             │
                     ┌─────────────────────┐                         │             │
                     │ Hourly Aggregated   ├────────────────────────►│             │
                     │ Quotes Topic        │                         │             │
                     └─────────────────────┘                         └─────────────┘
```

## Features

- **Real-time processing**: Processes stock quotes as they arrive
- **Currency conversion**: Converts USD prices to ILS (Israeli Shekel)
- **Moving averages**: Calculates moving averages for each stock ticker
- **Hourly aggregation**: Tracks the number of quotes received per hour per ticker
- **Deduplication**: Prevents processing duplicate or out-of-order messages
- **Snowflake integration**: Stores processed data in Snowflake for analytics
- **Data simulation**: Includes a simulator to generate test data at various speeds

## Data Models

### StockQuote

Represents a single stock quote with enrichments:

- `ticker`: Stock symbol (e.g., "AAPL")
- `price`: Stock price in USD
- `timestamp`: Timestamp in milliseconds
- `price_ils`: Price converted to Israeli Shekels
- `moving_avg`: Moving average of the last 3 prices

### HourlyAggregation

Tracks the number of quotes per ticker per hour:

- `ticker`: Stock symbol
- `hour_timestamp`: Timestamp rounded down to the hour
- `quote_count`: Number of quotes received in that hour

## Kafka Topics

- `stock-quotes`: Raw incoming stock quotes
- `enriched-stock-quotes`: Processed and enriched stock quotes
- `hourly-aggregated-quotes`: Hourly aggregation statistics

## Usage

### Processing Mode

To run the processor:

```bash
python python_stream_processor.py --mode process --kafka localhost:9092 --config ./snowflake.properties
```

### Simulation Mode

To generate test data:

```bash
python python_stream_processor.py --mode simulate --kafka localhost:9092 --count 1000 --speed SLOW
```

Speed options:
- `MAX`: No delay between messages (maximum throughput)
- `SLOW`: ~1000 messages per second
- `SLOOW`: ~100 messages per second
- `SLOOOW`: ~10 messages per second
- `SLOOOOW`: ~1 message per second

## Configuration

### Snowflake Connection

Create a `snowflake.properties` file with the following properties:

```
account=your_account
user=your_username
password=your_password
warehouse=your_warehouse
database=your_database
schema=your_schema
role=your_role
```

## Requirements

- Python 3.7+
- confluent-kafka
- snowflake-snowpark-python
- requests

## Notes

- The system uses a default exchange rate (3.7) if the external API is unavailable
- Moving averages are calculated over the last 3 price points
- Hourly aggregations are flushed to Kafka and Snowflake every 60 seconds