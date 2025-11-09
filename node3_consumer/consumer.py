import sys
import time
import requests
from kafka import KafkaConsumer
import json

# =========================================================================
# \u26a0\ufe0f CONFIGURATION: REPLACE THESE IPs WITH YOUR ACTUAL ZEROTIER IPs \u26a0\ufe0f
# =========================================================================
# These must match the addresses used by Node 1 and Node 4
KAFKA_BROKER_URL = '172.28.81.85:9092'  # <-- Replace with your Node 2 IP
ADMIN_APP_URL = 'http://172.28.15.11:5000' # <-- Replace with your Node 4 IP
# =========================================================================

def get_subscriptions(user_id):
    """
    Fetches the list of active topics this user is subscribed to from the Admin App API.
    """
    try:
        # Calls the Node 4 endpoint: /api/subscriptions/<user_id>
        response = requests.get(f"{ADMIN_APP_URL}/api/subscriptions/{user_id}", timeout=5)
        response.raise_for_status()
        
        data = response.json()
        
        # The API is designed to return a list of topic names under 'subscribed_topics'
        return set(data.get('subscribed_topics', []))
        
    except requests.exceptions.RequestException as e:
        # This handles network errors, timeouts, and 4xx/5xx responses
        print(f"[ConsumerError] Could not connect to Admin App (Node 4) for subscription check: {e}")
        return set() # Return an empty set if connection fails to avoid crashes

def run_consumer(user_id):
    """
    Main loop for the dynamic consumer.
    """
    print(f"--- Starting consumer for user: {user_id} ---")
    print(f"Connecting to Kafka at {KAFKA_BROKER_URL}")

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Start reading from the latest offset to get real-time data
        auto_offset_reset='latest', 
        enable_auto_commit=True,
        group_id=f'consumer-group-{user_id}', # Unique group ID for each user
        value_deserializer=lambda v: v.decode('utf-8')
    )

    current_subscriptions = set()
    DB_POLL_INTERVAL = 5 # Check the database every 5 seconds for subscription changes

    while True:
        # 1. Check for subscription changes by polling the database (Node 4)
        try:
            new_subscriptions = get_subscriptions(user_id)

            if new_subscriptions != current_subscriptions:
                topic_list = list(new_subscriptions)
                print(f"\n[SubscriptionChange] Updating subscriptions from {current_subscriptions} to: {topic_list}\n")
                
                # Dynamic subscription update using Kafka Consumer API
                consumer.subscribe(topics=topic_list)
                current_subscriptions = new_subscriptions

            # 2. Poll Kafka for messages
            # poll() waits up to 1 second (1000ms) for messages
            msg_pack = consumer.poll(timeout_ms=1000)

            for tp, messages in msg_pack.items():
                for msg in messages:
                    print(f"[{user_id} @ {msg.topic}] >> {msg.value}")

        except Exception as e:
            print(f"[ConsumerError] An unhandled error occurred: {e}")
            time.sleep(1) # Short pause on error

        # Wait to poll the DB again (this pause is only active when Kafka poll is fast)
        time.sleep(DB_POLL_INTERVAL) 

# --- Start the consumer ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: Must provide a user ID.")
        print("Usage: python consumer.py <user_id>")
        sys.exit(1)

    user_id = sys.argv[1]
    run_consumer(user_id)

