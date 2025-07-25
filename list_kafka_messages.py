#!/usr/bin/env python3
"""
Kafka Topic Message Lister

This script lists all messages from a specified Kafka topic.
"""

from kafka import KafkaConsumer
import json
import os
from time import time

def list_messages(topic_name, max_messages=None, timeout_ms=5000, client_id='kafka_topic_lister', group_id='kafka_topic_lister_group'):
    """
    List messages from a Kafka topic
    
    Args:
        topic_name (str): Name of the Kafka topic
        max_messages (int): Maximum number of messages to read (None for all)
    timeout_ms (int): Consumer timeout in milliseconds
    """

    timestamp = int(time()*1000)  # Get current timestamp in milliseconds
    timeout_ts = timestamp + timeout_ms

    def value_deserializer(value):
        if value is None:
            return None
        return value.decode('utf-8')

    # Get Kafka bootstrap servers from environment variable
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    bootstrap_servers = kafka_servers.split(',') if ',' in kafka_servers else [kafka_servers]
    
    print(f"🔗 Connecting to Kafka at: {bootstrap_servers}")

    consumer = KafkaConsumer(
        topic_name,
        client_id=client_id,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=value_deserializer,
        consumer_timeout_ms=timeout_ms,
        auto_offset_reset='earliest',  # Start from the beginning
        enable_auto_commit=True  # Don't commit offsets
    )
    
    print(f"📋 Listing messages from topic: {topic_name}")
    
    message_count = 0
    messages = []

    for message in consumer:

        if message.value is None:
            continue
        # print(f"\n📩 Message {message_count + 1}: \n")
        # print(f"{message}")
        message_count += 1
        
        # Try to parse as JSON for better formatting
        try:
            parsed_value = json.loads(message.value)
            formatted_value = json.dumps(parsed_value, indent=2)
        except (json.JSONDecodeError, TypeError):
            formatted_value = message.value
        json_value = json.loads(formatted_value)
        messages.append(json_value)

        # print(f"   Partition: {message.partition}")
        # print(f"   Offset: {message.offset}")
        # print(f"   Timestamp: {message.timestamp}")
        # print(f"   Key: {message.key}")
        # print(f"   Value:")
        # Indent the value for better readability
        # for line in formatted_value.split('\n'):
        #     print(f"     {line}")
        
        # Stop if we've reached the maximum number of messages
        if timeout_ts < int(time() * 1000):
            print(f"⏹️  Stopped after {timeout_ms} ms (timeout reached)")
            break

        if max_messages and message_count >= max_messages:
            print(f"\n⏹️  Stopped after {max_messages} messages (limit reached)")

            
            break
                

    
    consumer.close()
    
    if message_count == 0:
        print("📭 No messages found in the topic")
    else:
        print(f"\n✅ Total messages read: {message_count}")

    return messages
        

def list_available_topics():
    """List all available Kafka topics"""
    try:
        from kafka.admin import KafkaAdminClient
        
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092']
        )
        
        topics = admin_client.list_topics()
        print("📋 Available topics:")
        for topic in sorted(topics):
            print(f"  - {topic}")
        
        admin_client.close()
        return True
        
    except Exception as e:
        print(f"❌ Error listing topics: {e}")
        return False

def main(list_topics=False, topic_name=None, max_messages=100000):
    
    if list_topics:
        list_available_topics()
        return
    
    messages = list_messages(topic_name, max_messages)

    for message in messages:
        print(json.dumps(message, indent=2))

    return messages

if __name__ == "__main__":
    main(topic_name="testdb.public.users")
