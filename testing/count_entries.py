from kafka import KafkaConsumer, TopicPartition

# Kafka configuration
EXTERNAL_BROKER = "3.80.138.59:9092"
CONSUMER_TOPIC = "health_events"

def count_messages_in_topic():
    consumer = KafkaConsumer(
        bootstrap_servers=[EXTERNAL_BROKER],
        enable_auto_commit=False
    )
    
    # Fetch the partitions for the topic
    partitions = consumer.partitions_for_topic(CONSUMER_TOPIC)
    if not partitions:
        print(f"No partitions found for topic {CONSUMER_TOPIC}")
        return 0

    total_count = 0

    for partition in partitions:
        tp = TopicPartition(CONSUMER_TOPIC, partition)

        # Fetch earliest and latest offsets for the partition
        consumer.assign([tp])
        consumer.seek_to_beginning(tp)
        start_offset = consumer.position(tp)

        consumer.seek_to_end(tp)
        end_offset = consumer.position(tp)

        # Count messages in this partition
        partition_count = end_offset - start_offset
        total_count += partition_count

        print(f"Partition {partition}: {partition_count} messages")

    consumer.close()
    print(f"Total messages in topic '{CONSUMER_TOPIC}': {total_count}")
    return total_count

if __name__ == "__main__":
    count_messages_in_topic()
