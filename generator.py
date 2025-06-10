import pandas as pd
from confluent_kafka import Producer
import json
import tldextract

# Configure Kafka producer
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

producer = Producer({
    'bootstrap.servers': 'localhost:29092'
})

# Load CSV dataset and debug
df = pd.read_csv('browser_history.csv')
print("DataFrame columns:", df.columns.tolist())
print("DataFrame head:\n", df.head())

# Process each row and send to Kafka
topic = 'browser-history'
for index, row in df.iterrows():
    url = row['url']
    title = row['title']
    # Combine date and time into last_visit_time
    last_visit = f"{row['date']} {row['time']}"
    domain = tldextract.extract(url).suffix
    message = {
        'url': url,
        'title': title,
        'last_visit': last_visit,
        'domain': domain,
        'visit_count': row['visitCount'],  # Optional: include visitCount
        'typed_count': row['typedCount']   # Optional: include typedCount
    }
    producer.produce(topic, value=json.dumps(message).encode('utf-8'), callback=delivery_report)
    producer.poll(0)  # Trigger delivery callbacks

# Flush and close producer
producer.flush()
# producer.close()

print(f"Sent {len(df)} messages to topic {topic}")