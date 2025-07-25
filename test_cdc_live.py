#!/usr/bin/env python3
"""
Test PostgreSQL CDC by making database changes

This script tests CDC by inserting, updating, and deleting data in PostgreSQL
and checking if the changes appear in Kafka.
"""

import psycopg2
import time
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
import json

load_dotenv()

def connect_to_postgres():
    """Connect to PostgreSQL"""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'testdb'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres')
    )

def test_cdc_operations(table_name = "users", update = False, delete = False):
    """Test INSERT, UPDATE, and DELETE operations"""
    print("=" * 50)
    
    # Connect to database
    conn = connect_to_postgres()
    cursor = conn.cursor()
    
    # Get current timestamp for unique data
    timestamp = int(time.time()*1000)
    
    # 1. INSERT operation
    test_username = f"cdc_test_user_{timestamp}"
    test_email = f"cdctest_{timestamp}@example.com"
    
    cursor.execute(f"""
        INSERT INTO testdb.public.{table_name} (username, email, first_name, last_name) 
        VALUES (%s, %s, 'CDC', 'Test')
        RETURNING id;
    """, (test_username, test_email))
    
    new_user_id = cursor.fetchone()[0]
    conn.commit()
    print(f"✅ Inserted user: {test_username} (ID: {new_user_id})")
    

    
    if update:
        # 2. UPDATE operation
        updated_first_name = f"Updated_{timestamp}"
        
        cursor.execute("""
            UPDATE users 
            SET first_name = %s, updated_at = CURRENT_TIMESTAMP 
            WHERE id = %s;
        """, (updated_first_name, new_user_id))
        
        conn.commit()
        print(f"✅ Updated user {new_user_id}: first_name = {updated_first_name}")

    if delete:
        # 3. DELETE operation
        
        cursor.execute("DELETE FROM users WHERE id = %s;", (new_user_id,))
        conn.commit()
        print(f"✅ Deleted user {new_user_id}")
        
        cursor.close()
        conn.close()
    
    return test_username, new_user_id

def check_cdc_messages(test_username, user_id):
    """Check if CDC messages appear in Kafka"""
    print(f"\n🔍 Checking CDC messages in Kafka...")
    
    consumer = KafkaConsumer(
        'testdb.public.users',
        bootstrap_servers=['localhost:9092'],
        consumer_timeout_ms=10000,
        auto_offset_reset='latest',  # Only check new messages
        enable_auto_commit=False,
        value_deserializer=lambda x: x.decode('utf-8')
    )
    
    messages_found = []
    message_count = 0
    
    print("🔍 Listening for CDC messages...")
    
    for message in consumer:
        message_count += 1
        try:
            data = json.loads(message.value)
            
            # Check if this message is related to our test
            after_data = data.get('after', {})
            before_data = data.get('before', {})
            operation = data.get('op', '')
            
            # Check if it's our test user
            is_our_user = False
            if after_data and str(after_data.get('id')) == str(user_id):
                is_our_user = True
            elif before_data and str(before_data.get('id')) == str(user_id):
                is_our_user = True
            elif after_data and after_data.get('username') == test_username:
                is_our_user = True
            elif before_data and before_data.get('username') == test_username:
                is_our_user = True
            
            if is_our_user:
                op_name = {
                    'c': 'CREATE',
                    'u': 'UPDATE', 
                    'd': 'DELETE',
                    'r': 'READ/SNAPSHOT'
                }.get(operation, operation)
                
                print(f"\n✅ Found CDC message - Operation: {op_name}")
                print(f"   Message {message_count}: partition={message.partition}, offset={message.offset}")
                print(f"   Operation: {operation}")
                
                if before_data:
                    print(f"   Before: {before_data}")
                if after_data:
                    print(f"   After: {after_data}")
                
                messages_found.append({
                    'operation': operation,
                    'before': before_data,
                    'after': after_data
                })
                
        except json.JSONDecodeError:
            continue
        
        # Stop after checking reasonable number of messages
        if message_count >= 10:
            break
    
    consumer.close()
    
    if messages_found:
        print(f"\n🎉 SUCCESS! Found {len(messages_found)} CDC message(s) for our test operations:")
        for i, msg in enumerate(messages_found, 1):
            op_name = {
                'c': 'INSERT',
                'u': 'UPDATE', 
                'd': 'DELETE'
            }.get(msg['operation'], msg['operation'])
            print(f"   {i}. {op_name} operation detected")
    else:
        print(f"\n⚠️  No CDC messages found for user {user_id}")
        print("This might mean:")
        print("- CDC is not working for real-time changes")
        print("- There's a delay in processing")
        print("- The connector is only doing snapshots")
    
    return len(messages_found) > 0

def main():
    print("🧪 PostgreSQL CDC Live Test")
    print("=" * 40)
    
    try:
        # Test database operations
        test_username, user_id = test_cdc_operations()
        
        # Wait a bit more for all messages to be processed
        print(f"\n⏰ Waiting 5 seconds for CDC to process all changes...")
        time.sleep(5)
        
        # Check CDC messages
        success = check_cdc_messages(test_username, user_id)
        
        print(f"\n" + "=" * 50)
        if success:
            print("🎉 CDC is working! PostgreSQL changes are appearing in Kafka.")
        else:
            print("❌ CDC might not be working for real-time changes.")
            print("💡 Try running the list_kafka_messages.py script to see all messages.")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    main()
