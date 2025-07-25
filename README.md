# Open Mirroring: PostgreSQL to Microsoft Fabric

A real-time data mirroring solution that streams PostgreSQL database changes to Microsoft Fabric using Microsoft Fabric´s Open Mirroring capability, Kafka and Debezium. This tool enables seamless synchronization of PostgreSQL tables with Microsoft Fabric's Mirrored Databases.

## 🚀 Features

- **Real-time CDC**: Captures PostgreSQL database changes using Debezium
- **Kafka Integration**: Streams data changes through Apache Kafka
- **Microsoft Fabric Support**: Direct integration with Fabric Mirrored Databases
- **Containerized Setup**: Easy deployment using Docker Compose
- **Multiple Table Support**: Mirror multiple PostgreSQL tables simultaneously

## 📋 Prerequisites

Before installing and running this solution, ensure you have:

- **Docker and Docker Compose** installed on your system
- **PostgreSQL server** running and accessible
- **Microsoft Fabric Workspace** and Mirrored Database set up
- **Azure Service Principal** with Contributor role on the Fabric Workspace

## 🛠️ Installation

### Step 1: Configure PostgreSQL for Logical Replication

Enable logical replication in your PostgreSQL configuration file (usually `postgresql.conf`):

```sql
wal_level = logical
max_wal_senders = 1
max_replication_slots = 1
```

**Important**: Restart PostgreSQL after making these changes.

### Step 2: Clone the Repository

```bash
git clone <repository-url>
cd postgresmirror
```

### Step 3: Configure Environment Variables

Edit the `docker-compose.yml` file and update the following environment variables:

#### PostgreSQL Connection
```yaml
POSTGRES_HOST: your-postgres-host        # Your PostgreSQL server host
POSTGRES_PORT: 5432                      # Your PostgreSQL port
POSTGRES_DB: your-database-name          # Your PostgreSQL database name
POSTGRES_USER: your-username             # Your PostgreSQL username
POSTGRES_PASSWORD: your-password         # Your PostgreSQL password
POSTGRES_TABLES: schema.table1,schema.table2  # Tables to mirror (comma-separated)
```

#### Microsoft Fabric & Authentification Configuration
```yaml
MIRRORING_ID: your-mirroring-id          # Fabric Mirrored Database ID
WORKSPACE_ID: your-workspace-id          # Fabric Workspace ID
CLIENT_SECRET: your-client-secret        # Service Principal Client Secret
CLIENT_ID: your-client-id                # Service Principal Client ID
TENANT_ID: your-tenant-id                # Service Principal Tenant ID
```

### Step 4: Start the Services

```bash
docker compose up -d
```

## 🔧 Usage

### Starting the Mirror Process

The mirroring process starts automatically when you run `docker compose up -d`. The system will:

1. Set up Debezium connectors for specified PostgreSQL tables
2. Stream changes to Kafka topics
3. Process and convert data to Parquet format which is compliant to Microsoft Fabric´s Open Mirroring standard
4. Upload data to Microsoft Fabric Mirrored Database´s landing zone.
5. From the Open Mirroring will pick up the changes and apply them to the Mirrored Database

### Changing the Debezium Connector and other configurations

To change the Debezium connector settings or other configurations, you can modify the following files and rebuild the mirror-postgres image
- manage_connector.py: This script configures and setups the Debezium connector
- list_kafka_messages.py: This script contains the extraction logic for the Kafka messages
- mirroringutils.py: This script contains utility functions for the mirroring process in general (not postgres specific)
- mirroring_postgres.py: This script contains the logic for the mirroring process specific to PostgreSQL
- mirrorpostgres.py: This script is the entry point for the mirror-postgres service

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.
