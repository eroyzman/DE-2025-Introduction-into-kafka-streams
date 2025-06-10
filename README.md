docker-compose up -d

docker exec -it docker-kafka-1 kafka-topics --create --topic browser-history --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

run generator.py

run consumer.py
