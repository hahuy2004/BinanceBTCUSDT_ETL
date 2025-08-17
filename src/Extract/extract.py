from concurrent.futures.thread import ThreadPoolExecutor
import requests
import json
import time
import logging
from datetime import datetime, timezone
from queue import Queue, Empty
from kafka import KafkaProducer

# --- Configuration ---
BINANCE_API_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
KAFKA_BROKER = 'localhost:9092' # Or 'broker:9094'
KAFKA_TOPIC_EXTRACT = 'btc-price'
TARGET_INTERVAL_SECONDS = 0.1
NUM_THREADS = 4
API_TIMEOUT_SECONDS = 2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=2,
        )
        logging.info(f"Connected to Kafka at {KAFKA_BROKER}, sending to topic '{KAFKA_TOPIC_EXTRACT}'")
        return producer
    except Exception as e:
        logging.error(f"Cannot connect to Kafka broker: {e}")
        return None

api_session = requests.Session()

def fetch_price():
    try:
        response = api_session.get(BINANCE_API_URL, timeout=API_TIMEOUT_SECONDS)
        response.raise_for_status()
        data = response.json()
        if 'symbol' in data and 'price' in data:
            try:
                data['price'] = float(data['price'])
                return data
            except ValueError:
                logging.warning(f"Invalid price value: {data.get('price')}")
    except Exception as e:
        logging.error(f"API fetch error: {e}")
    return None

def format_timestamp(precision_ms=100):
    event_time = datetime.now(timezone.utc)
    ms = event_time.microsecond // 1000
    rounded_ms = ms - (ms % precision_ms)
    event_time = event_time.replace(microsecond=rounded_ms * 1000)
    return event_time.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

# The newest queue contains only elements
class LatestOnlyQueue(Queue):
    def put(self, item, block=True, timeout=None):
        with self.mutex:
            self.queue.clear() # Chỉ giữ phần tử mới nhất
            self.queue.append(item)
            self.unfinished_tasks = len(self.queue)
            self.not_empty.notify()

data_queue = LatestOnlyQueue()

def fetch_worker():
    while True:
        data = fetch_price()
        if data:
            data_queue.put(data)
        time.sleep(0.01)

def main():
    producer = create_kafka_producer()
    if not producer:
        return

    try:
        executor = ThreadPoolExecutor(max_workers=NUM_THREADS)
        for _ in range(NUM_THREADS):
            executor.submit(fetch_worker)

        last_send = 0

        while True:
            try:
                data = data_queue.get(timeout=5)
                now = time.time()
                time_to_wait = TARGET_INTERVAL_SECONDS - (now - last_send)
                if time_to_wait > 0:
                    time.sleep(time_to_wait)

                last_send = time.time()

                data['timestamp'] = format_timestamp()

                producer.send(KAFKA_TOPIC_EXTRACT, value=data)
                logging.info(f"Sent: {data}")

            except Empty:
                logging.warning("No data received for 5 seconds.")
                break

    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
    finally:
        if producer:
            logging.info("Closing Kafka producer...")
            producer.flush(timeout=10)
            producer.close()
        if api_session:
            api_session.close()
            logging.info("API session closed.")

if __name__ == "__main__":
    main()
