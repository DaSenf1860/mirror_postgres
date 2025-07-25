from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient, DataLakeDirectoryClient
import os
import pyarrow as pa
import pyarrow.parquet as pq

def get_service_client_token_credential_(credential) -> DataLakeServiceClient:
    account_url = f"https://onelake.dfs.fabric.microsoft.com/"

    service_client = DataLakeServiceClient(account_url, credential=credential)

    return service_client


def list_directory_contents(file_system_client: FileSystemClient, directory_name: str):
    paths = file_system_client.get_paths(path=directory_name)

    return paths
# %%
def upload_file_to_directory(directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
    file_client = directory_client.get_file_client(file_name)

    with open(file=os.path.join(local_path, file_name), mode="rb") as data:
        file_client.upload_data(data, overwrite=True)

# def get_secret_(secret_name, keyvault_name):
#     vaultBaseUrl = f"https://{keyvault_name}.vault.azure.net"
#     keyvault_token = local_credentials.get_token(vaultBaseUrl).token
    
#     request_headers = {
#         "Authorization": f"Bearer {keyvault_token}",
#         "Content-Type": "application/json"
#     }
#     keyvault_url = f"{vaultBaseUrl}/secrets/{secret_name}?api-version=7.4"
#     response = requests.get(keyvault_url, headers=request_headers)
#     if response.status_code == 200:
#         secret_value = response.json()["value"]
#         return secret_value
#     else:
#         raise Exception(f"Failed to retrieve secret: {response.status_code} - {response.text}")

def get_latest_partition(fsc, mirrored_db_id, table_name):
    mirrored_db_path = f"{mirrored_db_id}/Files/LandingZone"


    table_path = mirrored_db_path + f"/{table_name}"
    contents = list_directory_contents(fsc, directory_name=table_path)
    content_names = [content["name"] for content in contents]
    partitionnames = [int(cont.split("/")[-1].split(".")[0]) for cont in content_names if cont.endswith(".parquet")]
    if len(partitionnames) < 1:
        latest_partition = 0
    else:
        latest_partition = max(partitionnames)
    latest_partition = str(latest_partition + 1)
    while len(latest_partition) < 20:
        latest_partition = "0" + latest_partition
    latest_partition = latest_partition + ".parquet"
    return latest_partition, table_path, fsc

def upload_parquet_to_landing_zone(df, schema, latest_partition, table_path, fsc, id_column="id"):
    os.makedirs('upload', exist_ok=True)

    table = pa.Table.from_pandas(df, schema=schema)
    pq.write_table(table, f"upload/{latest_partition}")
    
    dldc = fsc.get_directory_client(table_path)
    upload_file_to_directory(dldc, local_path="upload", file_name=latest_partition)
    count = df[id_column].count()
    print(f"Uploaded {latest_partition} with row count {count}")

def init_mirror_table(fsc, mirrored_db_id, table_name,id_column="id"):

    mirrored_db_path = f"{mirrored_db_id}/Files/LandingZone"
    fsc.create_directory(mirrored_db_path)

    contents = list_directory_contents(fsc, directory_name=mirrored_db_path)
    content_names = [content["name"] for content in contents]
    table_path = mirrored_db_path + f"/{table_name}"
    if table_path not in content_names:
        fsc.create_directory(table_path)
        print(f"Directory {table_path} created")

    contents = list_directory_contents(fsc, directory_name=mirrored_db_path)
    content_names = [content["name"] for content in contents]
    metadata_path = mirrored_db_path + f"/{table_name}/_metadata.json"
    if metadata_path not in content_names:
        import json, os
        json_content = {
                    "keyColumns": [
                    id_column
                    ],
                    "fileFormat": "parquet"
                }
        os.makedirs("upload", exist_ok=True)
        json.dump(json_content, open("upload/_metadata.json", "w"), indent=4)
        dldc = fsc.get_directory_client(table_path)
        upload_file_to_directory(dldc, local_path="upload", file_name="_metadata.json")
        print(f"File {metadata_path} uploaded")

def reset_mirror_table(fsc, mirrored_db_id, table_name, id_column="id"):
    mirrored_db_path = f"{mirrored_db_id}/Files/LandingZone"
    table_path = mirrored_db_path + f"/{table_name}"
    
    fsc.delete_directory(table_path)
    print(f"Directory {table_path} deleted")
    init_mirror_table(fsc, mirrored_db_id, table_name, id_column=id_column)