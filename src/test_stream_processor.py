import pytest
import json
import time
from unittest.mock import Mock, patch, MagicMock, mock_open
from collections import deque
from datetime import datetime
import os

# Import the classes from your module
from python_stream_processor import (
    StockQuote,
    HourlyAggregation,
    StockStreamProcessor,
    StockDataSimulator,
    load_config,
    ILS_EXCHANGE_RATE,
    MOVING_AVG_WINDOW_SIZE
)


class TestStockQuote:
    """Test cases for StockQuote class"""

    def test_stock_quote_creation(self):
        """Test basic StockQuote creation"""
        quote = StockQuote("AAPL", 150.50, 1640995200000)
        assert quote.ticker == "AAPL"
        assert quote.price == 150.50
        assert quote.timestamp == 1640995200000
        assert quote.price_ils is None
        assert quote.moving_avg is None

    def test_stock_quote_to_dict(self):
        """Test StockQuote to_dict method"""
        quote = StockQuote("MSFT", 300.25, 1640995200000)
        quote.price_ils = 1111.93
        quote.moving_avg = 295.5

        expected = {
            "ticker": "MSFT",
            "price": 300.25,
            "timestamp": 1640995200000,
            "price_ils": 1111.93,
            "moving_avg": 295.5
        }
        assert quote.to_dict() == expected

    def test_stock_quote_from_dict(self):
        """Test StockQuote from_dict class method"""
        data = {
            "ticker": "GOOG",
            "price": 2800.0,
            "timestamp": 1640995200000,
            "price_ils": 10360.0,
            "moving_avg": 2750.5
        }

        quote = StockQuote.from_dict(data)
        assert quote.ticker == "GOOG"
        assert quote.price == 2800.0
        assert quote.timestamp == 1640995200000
        assert quote.price_ils == 10360.0
        assert quote.moving_avg == 2750.5

    def test_stock_quote_from_dict_minimal(self):
        """Test StockQuote from_dict with minimal data"""
        data = {
            "ticker": "TSLA",
            "price": 1000.0,
            "timestamp": 1640995200000
        }

        quote = StockQuote.from_dict(data)
        assert quote.ticker == "TSLA"
        assert quote.price == 1000.0
        assert quote.timestamp == 1640995200000
        assert quote.price_ils is None
        assert quote.moving_avg is None

    def test_stock_quote_str(self):
        """Test StockQuote string representation"""
        quote = StockQuote("AMZN", 3200.0, 1640995200000)
        quote.price_ils = 11840.0
        quote.moving_avg = 3150.0

        expected = ("StockQuote(ticker=AMZN, price=3200.0, "
                    "timestamp=1640995200000, price_ils=11840.0, "
                    "moving_avg=3150.0)")
        assert str(quote) == expected


class TestHourlyAggregation:
    """Test cases for HourlyAggregation class"""

    def test_hourly_aggregation_creation(self):
        """Test basic HourlyAggregation creation"""
        agg = HourlyAggregation("AAPL")
        assert agg.ticker == "AAPL"
        assert agg.hour_timestamp == 0
        assert agg.quote_count == 0

    def test_hourly_aggregation_with_params(self):
        """Test HourlyAggregation creation with parameters"""
        agg = HourlyAggregation("MSFT", 1640995200000, 5)
        assert agg.ticker == "MSFT"
        assert agg.hour_timestamp == 1640995200000
        assert agg.quote_count == 5

    def test_increment_count(self):
        """Test increment_count method"""
        agg = HourlyAggregation("GOOG")
        assert agg.quote_count == 0

        agg.increment_count()
        assert agg.quote_count == 1

        agg.increment_count()
        assert agg.quote_count == 2

    def test_to_dict(self):
        """Test to_dict method"""
        agg = HourlyAggregation("TSLA", 1640995200000, 10)
        expected = {
            "ticker": "TSLA",
            "hour_timestamp": 1640995200000,
            "quote_count": 10
        }
        assert agg.to_dict() == expected

    def test_str(self):
        """Test string representation"""
        agg = HourlyAggregation("NFLX", 1640995200000, 3)
        expected = ("HourlyAggregation(ticker=NFLX, "
                    "hour_timestamp=1640995200000, quote_count=3)")
        assert str(agg) == expected


class TestStockStreamProcessor:
    """Test cases for StockStreamProcessor class"""

    @pytest.fixture
    def processor(self):
        """Create a StockStreamProcessor instance for testing"""
        with patch('python_stream_processor.Producer'), \
                patch('python_stream_processor.Session'):
            return StockStreamProcessor("localhost:9092")

    @pytest.fixture
    def processor_with_snowflake(self):
        """Create a StockStreamProcessor with Snowflake config"""
        snowflake_config = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password"
        }
        with patch('python_stream_processor.Producer'), \
                patch('python_stream_processor.Session') as mock_session:
            mock_session.builder.configs.return_value.create.return_value = Mock()
            return StockStreamProcessor("localhost:9092", snowflake_config)

    def test_processor_initialization(self, processor):
        """Test processor initialization"""
        assert processor.kafka_bootstrap_servers == "localhost:9092"
        assert processor.snowflake_config is None
        assert len(processor.price_history) == 0
        assert len(processor.processed_timestamps) == 0
        assert len(processor.hourly_aggregations) == 0
        assert processor.snowflake_session is None

    def test_processor_with_snowflake_config(self, processor_with_snowflake):
        """Test processor initialization with Snowflake config"""
        assert processor_with_snowflake.snowflake_config is not None
        assert processor_with_snowflake.snowflake_session is not None

    @patch('python_stream_processor.requests.get')
    def test_get_exchange_rate_success(self, mock_get, processor):
        """Test successful exchange rate retrieval"""
        mock_response = Mock()
        mock_response.json.return_value = {"rates": {"ILS": 3.5}}
        mock_get.return_value = mock_response

        rate = processor.get_exchange_rate()
        assert rate == 3.5
        mock_get.assert_called_once_with("https://api.exchangerate-api.com/v4/latest/USD", timeout=5)

    @patch('python_stream_processor.requests.get')
    def test_get_exchange_rate_failure(self, mock_get, processor):
        """Test exchange rate retrieval failure fallback"""
        mock_get.side_effect = Exception("API Error")

        rate = processor.get_exchange_rate()
        assert rate == ILS_EXCHANGE_RATE  # Should return fallback constant

    def test_calculate_moving_average_insufficient_data(self, processor):
        """Test moving average with insufficient data points"""
        avg = processor.calculate_moving_average("AAPL", 100.0)
        assert avg is None

        avg = processor.calculate_moving_average("AAPL", 110.0)
        assert avg is None  # Still only 2 data points

    def test_calculate_moving_average_sufficient_data(self, processor):
        """Test moving average with sufficient data points"""
        # Add first two prices
        processor.calculate_moving_average("AAPL", 100.0)
        processor.calculate_moving_average("AAPL", 110.0)

        # Third price should return moving average
        avg = processor.calculate_moving_average("AAPL", 120.0)
        expected_avg = round((100.0 + 110.0 + 120.0) / 3, 1)
        assert avg == expected_avg

    def test_calculate_moving_average_window_size(self, processor):
        """Test that moving average respects window size"""
        # Add more prices than window size
        prices = [100.0, 110.0, 120.0, 130.0, 140.0]

        for price in prices:
            avg = processor.calculate_moving_average("AAPL", price)

        # Should only consider last MOVING_AVG_WINDOW_SIZE prices
        expected_avg = round((120.0 + 130.0 + 140.0) / 3, 1)
        assert avg == expected_avg

    def test_update_hourly_aggregation_new_ticker(self, processor):
        """Test hourly aggregation update for new ticker"""
        timestamp = 1640995200000  # Some timestamp
        hour_timestamp = (timestamp // 3600000) * 3600000

        processor.update_hourly_aggregation("AAPL", timestamp)

        key = f"AAPL_{hour_timestamp}"
        assert key in processor.hourly_aggregations
        assert processor.hourly_aggregations[key].ticker == "AAPL"
        assert processor.hourly_aggregations[key].hour_timestamp == hour_timestamp
        assert processor.hourly_aggregations[key].quote_count == 1

    def test_update_hourly_aggregation_existing_ticker(self, processor):
        """Test hourly aggregation update for existing ticker"""
        timestamp = 1640995200000
        hour_timestamp = (timestamp // 3600000) * 3600000

        # First update
        processor.update_hourly_aggregation("AAPL", timestamp)
        # Second update in same hour
        processor.update_hourly_aggregation("AAPL", timestamp + 1000)

        key = f"AAPL_{hour_timestamp}"
        assert processor.hourly_aggregations[key].quote_count == 2

    @patch('python_stream_processor.StockStreamProcessor.get_exchange_rate')
    def test_enrich_quote(self, mock_exchange_rate, processor):
        """Test quote enrichment"""
        mock_exchange_rate.return_value = 3.5

        quote = StockQuote("AAPL", 100.0, 1640995200000)
        enriched = processor.enrich_quote(quote)

        assert enriched.price_ils == 350.0  # 100.0 * 3.5
        assert enriched.moving_avg is None  # First quote, no moving average yet

        # Check that hourly aggregation was updated
        hour_timestamp = (1640995200000 // 3600000) * 3600000
        key = f"AAPL_{hour_timestamp}"
        assert key in processor.hourly_aggregations

    def test_process_message_valid(self, processor):
        """Test processing valid message"""
        with patch.object(processor, 'enrich_quote') as mock_enrich:
            mock_quote = StockQuote("AAPL", 100.0, 1640995200000)
            mock_enrich.return_value = mock_quote

            message = {
                "ticker": "AAPL",
                "price": "100.0",
                "timestamp": "1640995200000"
            }

            result = processor.process_message(message)
            assert result == mock_quote
            mock_enrich.assert_called_once()

    def test_process_message_invalid_data(self, processor):
        """Test processing invalid message data"""
        # Missing ticker
        message1 = {"price": "100.0", "timestamp": "1640995200000"}
        assert processor.process_message(message1) is None

        # Invalid price
        message2 = {"ticker": "AAPL", "price": "0", "timestamp": "1640995200000"}
        assert processor.process_message(message2) is None

        # Negative price
        message3 = {"ticker": "AAPL", "price": "-100", "timestamp": "1640995200000"}
        assert processor.process_message(message3) is None

    def test_process_message_deduplication(self, processor):
        """Test message deduplication"""
        with patch.object(processor, 'enrich_quote') as mock_enrich:
            mock_quote = StockQuote("AAPL", 100.0, 1640995200000)
            mock_enrich.return_value = mock_quote

            message1 = {
                "ticker": "AAPL",
                "price": "100.0",
                "timestamp": "1640995200000"
            }

            message2 = {
                "ticker": "AAPL",
                "price": "110.0",
                "timestamp": "1640995100000"  # Earlier timestamp
            }

            # Process first message
            result1 = processor.process_message(message1)
            assert result1 == mock_quote

            # Process second message with earlier timestamp (should be skipped)
            result2 = processor.process_message(message2)
            assert result2 is None

            # Should only have called enrich_quote once
            assert mock_enrich.call_count == 1

    def test_send_to_kafka(self, processor):
        """Test sending message to Kafka"""
        test_data = {"ticker": "AAPL", "price": 100.0}

        processor.send_to_kafka("test-topic", "AAPL", test_data)

        # Verify producer.produce was called
        processor.producer.produce.assert_called_once()
        args, kwargs = processor.producer.produce.call_args

        assert kwargs['topic'] == "test-topic"
        assert kwargs['key'] == b"AAPL"
        assert json.loads(kwargs['value'].decode('utf-8')) == test_data

    def test_save_to_snowflake_no_connection(self, processor):
        """Test saving to Snowflake without connection"""
        result = processor.save_to_snowflake("test_table", [{"data": "test"}])
        assert result is False

    def test_save_to_snowflake_with_connection(self, processor_with_snowflake):
        """Test saving to Snowflake with connection"""
        mock_df = Mock()
        processor_with_snowflake.snowflake_session.create_dataframe.return_value = mock_df

        test_data = [{"ticker": "AAPL", "price": 100.0}]
        result = processor_with_snowflake.save_to_snowflake("test_table", test_data)

        assert result is True
        processor_with_snowflake.snowflake_session.create_dataframe.assert_called_once_with(test_data)
        mock_df.write.mode.assert_called_once_with("append")

    def test_flush_hourly_aggregations_empty(self, processor):
        """Test flushing empty hourly aggregations"""
        # Should not raise any errors
        processor.flush_hourly_aggregations()

        # Producer should not be called
        processor.producer.produce.assert_not_called()

    def test_flush_hourly_aggregations_with_data(self, processor):
        """Test flushing hourly aggregations with data"""
        # Add some aggregations
        processor.update_hourly_aggregation("AAPL", 1640995200000)
        processor.update_hourly_aggregation("MSFT", 1640995200000)

        processor.flush_hourly_aggregations()

        # Should have called produce for each aggregation
        assert processor.producer.produce.call_count == 2

        # Aggregations should be cleared
        assert len(processor.hourly_aggregations) == 0


class TestStockDataSimulator:
    """Test cases for StockDataSimulator class"""

    @pytest.fixture
    def simulator(self):
        """Create a StockDataSimulator instance for testing"""
        with patch('python_stream_processor.Producer') as mock_producer:
            return StockDataSimulator("localhost:9092", "MAX")

    def test_simulator_initialization(self, simulator):
        """Test simulator initialization"""
        assert simulator.speed == "MAX"
        assert len(simulator.tickers) > 0
        assert "AAPL" in simulator.tickers

    def test_simulator_speed_conversion(self):
        """Test that speed is converted to uppercase"""
        with patch('python_stream_processor.Producer'):
            simulator = StockDataSimulator("localhost:9092", "slow")
            assert simulator.speed == "SLOW"

    @patch('python_stream_processor.datetime')
    @patch('python_stream_processor.random')
    def test_generate_stock_data(self, mock_random, mock_datetime, simulator):
        """Test stock data generation"""
        # Mock random choices and values
        mock_random.choice.return_value = "AAPL"
        mock_random.uniform.return_value = 150.50

        # Mock timestamp
        mock_datetime.timestamp.return_value = 1640995200.0
        mock_datetime.now.return_value = Mock()

        simulator.generate_stock_data(count=2)

        # Should have called produce twice
        assert simulator.producer.produce.call_count == 2

        # Verify flush was called
        simulator.producer.flush.assert_called_once()

    @patch('python_stream_processor.time.sleep')
    @patch('python_stream_processor.datetime')
    @patch('python_stream_processor.random')
    def test_generate_stock_data_with_delay(self, mock_random, mock_datetime, mock_sleep, simulator):
        """Test stock data generation with delay"""
        simulator.speed = "SLOW"

        mock_random.choice.return_value = "AAPL"
        mock_random.uniform.return_value = 150.50
        mock_datetime.timestamp.return_value = 1640995200.0
        mock_datetime.now.return_value = Mock()

        simulator.generate_stock_data(count=2)

        # Should have called sleep for each message
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(0.001)  # SLOW speed delay


class TestLoadConfig:
    """Test cases for load_config function"""

    def test_load_config_file_exists(self):
        """Test loading config when file exists"""
        config_content = "user=test_user\npassword=test_password\n# comment line\naccount=test_account"

        with patch("builtins.open", mock_open(read_data=config_content)):
            with patch("os.path.exists", return_value=True):
                config = load_config("test.properties")

        expected = {
            "user": "test_user",
            "password": "test_password",
            "account": "test_account"
        }
        assert config == expected

    def test_load_config_file_not_exists(self):
        """Test loading config when file doesn't exist"""
        with patch("os.path.exists", return_value=False):
            config = load_config("nonexistent.properties")

        assert config == {}

    def test_load_config_file_read_error(self):
        """Test loading config when file read fails"""
        with patch("os.path.exists", return_value=True):
            with patch("builtins.open", side_effect=IOError("File read error")):
                config = load_config("test.properties")

        assert config == {}


# Integration Tests
class TestIntegration:
    """Integration tests for the complete workflow"""

    @patch('python_stream_processor.Producer')
    @patch('python_stream_processor.requests.get')
    def test_end_to_end_quote_processing(self, mock_get, mock_producer):
        """Test complete quote processing workflow"""
        # Mock exchange rate API
        mock_response = Mock()
        mock_response.json.return_value = {"rates": {"ILS": 3.5}}
        mock_get.return_value = mock_response

        # Create processor
        processor = StockStreamProcessor("localhost:9092")

        # Process multiple quotes for same ticker to test moving average
        messages = [
            {"ticker": "AAPL", "price": "100.0", "timestamp": "1640995200000"},
            {"ticker": "AAPL", "price": "110.0", "timestamp": "1640995260000"},
            {"ticker": "AAPL", "price": "120.0", "timestamp": "1640995320000"},
        ]

        results = []
        for msg in messages:
            result = processor.process_message(msg)
            if result:
                results.append(result)

        # Should have 3 processed quotes
        assert len(results) == 3

        # Check first quote (no moving average yet)
        assert results[0].ticker == "AAPL"
        assert results[0].price == 100.0
        assert results[0].price_ils == 350.0  # 100 * 3.5
        assert results[0].moving_avg is None

        # Check third quote (should have moving average)
        assert results[2].moving_avg == 110.0  # (100 + 110 + 120) / 3

        # Check hourly aggregation
        hour_timestamp = (1640995200000 // 3600000) * 3600000
        key = f"AAPL_{hour_timestamp}"
        assert key in processor.hourly_aggregations
        assert processor.hourly_aggregations[key].quote_count == 3


# Fixtures for common test data
@pytest.fixture
def sample_stock_quotes():
    """Sample stock quotes for testing"""
    return [
        {"ticker": "AAPL", "price": 150.0, "timestamp": 1640995200000},
        {"ticker": "MSFT", "price": 300.0, "timestamp": 1640995260000},
        {"ticker": "GOOG", "price": 2800.0, "timestamp": 1640995320000},
    ]


@pytest.fixture
def sample_config():
    """Sample Snowflake configuration"""
    return {
        "account": "test_account",
        "user": "test_user",
        "password": "test_password",
        "warehouse": "test_warehouse",
        "database": "test_database",
        "schema": "test_schema"
    }


# Performance Tests
class TestPerformance:
    """Performance-related tests"""

    def test_large_batch_processing(self):
        """Test processing large batch of messages"""
        with patch('python_stream_processor.Producer'), \
                patch('python_stream_processor.requests.get') as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = {"rates": {"ILS": 3.5}}
            mock_get.return_value = mock_response

            processor = StockStreamProcessor("localhost:9092")

            # Process 1000 messages
            start_time = time.time()
            for i in range(1000):
                message = {
                    "ticker": f"STOCK{i % 10}",
                    "price": str(100.0 + i),
                    "timestamp": str(1640995200000 + i * 1000)
                }
                processor.process_message(message)

            end_time = time.time()
            processing_time = end_time - start_time

            # Should process reasonably quickly (adjust threshold as needed)
            assert processing_time < 5.0  # Less than 5 seconds for 1000 messages

            # Should have data in price history for multiple tickers
            assert len(processor.price_history) == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])