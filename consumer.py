from confluent_kafka import Consumer
from collections import Counter
import json

# Configure Kafka consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'domain-stats-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
})

# Subscribe to topic
consumer.subscribe(['browser-history'])

# Track domain counts based on visit_count
domain_counts = Counter()

# Process messages
try:
    print("Starting to consume messages from browser-history topic...")
    while True:
        msg = consumer.poll(1.0)  # Poll for messages with 1-second timeout
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        # Decode and parse the message
        data = json.loads(msg.value().decode('utf-8'))
        domain = data['domain']
        visit_count = data.get('visit_count', 1)  # Default to 1 if missing
        domain_counts[domain] += visit_count
        # Print top 5 domains periodically (e.g., every 5 messages)
        if sum(domain_counts.values()) % 5 == 0:
            top_domains = domain_counts.most_common(5)
            print("Top 5 root domains by visit count:")
            for domain, count in top_domains:
                print(f"{domain}: {count}")
except KeyboardInterrupt:
    print("Consumer stopped by user")
finally:
    # Print final top 5 and close consumer
    top_domains = domain_counts.most_common(5)
    print("Final top 5 root domains by visit count:")
    for domain, count in top_domains:
        print(f"{domain}: {count}")
    consumer.close()