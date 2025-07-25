#!/usr/bin/env python3
"""
Debezium Connector Status Checker

This script checks the status of Debezium connectors and helps troubleshoot issues.
"""

import requests
import json
import time

from dotenv import load_dotenv
import os

load_dotenv()

def check_connector_status():
    """Check the status of all connectors in detail"""
    print("🔍 Checking Debezium Connector Status")
    print("=" * 50)
    
    try:
        # Get all connectors
        response = requests.get('http://kafka-connect:8083/connectors', timeout=5)
        if response.status_code != 200:
            print(f"❌ Failed to get connectors: {response.status_code}")
            return
        
        connectors = response.json()
        print(f"🔌 Found {len(connectors)} connector(s): {connectors}")
        
        for connector_name in connectors:
            print(f"\n📊 Connector: {connector_name}")
            
            # Get connector status
            status_response = requests.get(f'http://kafka-connect:8083/connectors/{connector_name}/status', timeout=5)
            if status_response.status_code == 200:
                status = status_response.json()
                
                # Connector state
                connector_state = status.get('connector', {}).get('state', 'unknown')
                print(f"   Connector State: {connector_state}")
                
                if connector_state != 'RUNNING':
                    print(f"   ⚠️  Connector is not running!")
                
                # Task states
                tasks = status.get('tasks', [])
                print(f"   Tasks: {len(tasks)}")
                
                for i, task in enumerate(tasks):
                    task_state = task.get('state', 'unknown')
                    task_id = task.get('id', i)
                    print(f"     Task {task_id}: {task_state}")
                    
                    if task_state == 'FAILED':
                        print(f"     ❌ Task failed!")
                        if 'trace' in task:
                            print(f"     Error: {task['trace'][:200]}...")
            else:
                print(f"   ❌ Failed to get status: {status_response.status_code}")
            
            # Get connector configuration
            config_response = requests.get(f'http://kafka-connect:8083/connectors/{connector_name}/config', timeout=5)
            if config_response.status_code == 200:
                config = config_response.json()
                print(f"   📋 Configuration:")
                important_configs = [
                    'database.hostname', 'database.dbname', 'table.include.list', 
                    'slot.name', 'publication.name', 'snapshot.mode'
                ]
                for key in important_configs:
                    if key in config:
                        print(f"     {key}: {config[key]}")
            
    except Exception as e:
        print(f"❌ Error checking connector status: {e}")

def restart_connector(connector_name):
    """Restart a connector"""
    print(f"\n🔄 Restarting connector: {connector_name}")
    
    try:
        response = requests.post(f'http://kafka-connect:8083/connectors/{connector_name}/restart', timeout=10)
        if response.status_code in [204, 202]:
            print(f"✅ Connector restart initiated")
            return True
        else:
            print(f"❌ Failed to restart connector: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error restarting connector: {e}")
        return False

def delete_connector(connector_name):
    """Delete a connector"""
    print(f"\n🗑️  Deleting connector: {connector_name}")
    
    try:
        response = requests.delete(f'http://kafka-connect:8083/connectors/{connector_name}', timeout=10)
        if response.status_code in [204, 202]:
            print(f"✅ Connector deleted")
            return True
        else:
            print(f"❌ Failed to delete connector: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error deleting connector: {e}")
        return False

def recreate_connector():
    """Delete and recreate the postgres connector with optimal settings"""
    print("\n🔧 Recreating Debezium connector with optimal settings...")
    
    # First delete existing connector
    delete_connector("postgres-connector")
    
    # Wait a bit
    time.sleep(2)
    
    # Create new connector with better configuration

    postgres_host = os.getenv("POSTGRES_HOST", "localhost")
    postgres_db = os.getenv("POSTGRES_DB")
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_tables = os.getenv("POSTGRES_TABLES")


    connector_config = {
        "name": "postgres-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": postgres_host,
            "database.port": "5432",
            "database.user": postgres_user,
            "database.password": postgres_password,
            "database.dbname": postgres_db,
            "database.server.name": postgres_db,
            "topic.prefix": postgres_db,
            "table.include.list": postgres_tables,
            "plugin.name": "pgoutput",
            "publication.name": "debezium_publication",
            "slot.name": "debezium_slot",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
            "snapshot.mode": "always",
            "tombstones.on.delete": "false",
            "decimal.handling.mode": "string",
            "include.schema.changes": "false",
            "provide.transaction.metadata": "false"
        }
    }
    
    try:
        response = requests.post(
            'http://kafka-connect:8083/connectors',
            headers={'Content-Type': 'application/json'},
            data=json.dumps(connector_config),
            timeout=10
        )
        
        if response.status_code in [201, 409]:
            print(f"✅ Connector recreated successfully: {response.status_code}")
            return True
        else:
            print(f"❌ Failed to recreate connector: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Failed to recreate connector: {e}")
        return False

def main():
    print("🔧 Debezium Connector Management")
    print("=" * 40)
    
    # Check current status
    check_connector_status()
    
    # Ask user what to do
    print("\nWhat would you like to do?")
    print("1. Restart existing connector")
    print("2. Recreate connector with optimal settings")
    print("3. Just check status (done above)")
    
    choice = input("Enter choice (1-3) or press Enter to continue: ").strip()
    
    if choice == "1":
        restart_connector("postgres-connector")
        # Wait and check status again
        time.sleep(5)
        check_connector_status()
    elif choice == "2":
        recreate_connector()
        # Wait and check status again
        time.sleep(5)
        check_connector_status()
    
    print("\n✅ Done! You can now test your CDC again.")

if __name__ == "__main__":
    main()
