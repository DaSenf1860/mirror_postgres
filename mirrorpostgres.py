from mirroring_postgres_utils import get_parquet
from mirroring_utils import get_service_client_token_credential_, init_mirror_table, reset_mirror_table, get_latest_partition, upload_parquet_to_landing_zone
from manage_connector import recreate_connector
from azure.identity import ClientSecretCredential
from time import time, sleep
from dotenv import load_dotenv
import os 

load_dotenv(override=True)

postgres_db = os.getenv("POSTGRES_DB")
table_names = os.getenv("POSTGRES_TABLES").split(",")
mirroring_id = os.getenv("MIRRORING_ID")
workspace_id = os.getenv("WORKSPACE_ID")
client_secret = os.getenv("CLIENT_SECRET")
client_id = os.getenv("CLIENT_ID")
tenant_id = os.getenv("TENANT_ID")

max_messages = os.getenv("MAX_MESSAGES", 10000)
max_messages = int(max_messages)
print(f"Max messages to process: {max_messages}")
timeout =  os.getenv("TIMEOUT", 5000)
timeout = int(timeout)
print(f"Timeout for processing messages: {timeout} ms")


credentials = ClientSecretCredential(tenant_id, client_id, client_secret)

timestamp = int(time())

client_id = f"kafka_topic_lister_{timestamp}"
group_id = f"kafka_topic_lister_group_{timestamp}"



for _ in range(6):
    try:
        result = recreate_connector()
        if result:
            break
    except Exception as e:
        print(f"Error recreating connector: {e}")
    finally:
        sleep(5)

dlsc = get_service_client_token_credential_(credentials)
fsc = dlsc.get_file_system_client(workspace_id)
for table_name in table_names:
    table_name_split = table_name.split(".")
    if len(table_name_split) == 2:
        schema_name = table_name_split[0]
        table_name_fab = table_name_split[1]
    else:
        table_name_fab = table_name_split[0]
    print(f"Resetting mirror table: {table_name_fab}")
    reset_mirror_table(fsc, mirroring_id, table_name_fab)


while True:
    for table_name in table_names:
        print(f"Processing table: {table_name}")

        df_concat, schema = get_parquet(postgres_db, table_name,
                                        client_id=client_id, group_id=group_id,max_messages=max_messages, timeout_ms=timeout)

        if df_concat.empty:
            print("No data found for the specified table.")
        else:
            dlsc = get_service_client_token_credential_(credentials)
            fsc = dlsc.get_file_system_client(workspace_id)
            table_name_split = table_name.split(".")
            if len(table_name_split) == 2:
                schema_name = table_name_split[0]
                table_name = table_name_split[1]
            init_mirror_table(fsc, mirroring_id, table_name, id_column="id")
            latest_partition, table_path, fsc = get_latest_partition(fsc, mirroring_id, table_name)
            upload_parquet_to_landing_zone(df_concat,schema, latest_partition, table_path, fsc)

        timestamp = int(time())
