import psycopg2
import os
from dotenv import load_dotenv
import pyarrow as pa
import pandas as pd
from decimal import Decimal

from list_kafka_messages import list_messages

# Load environment variables from .env file
load_dotenv()

def get_postgres_schema(table_name="users", schema_name="public"):
    """
    Retrieve the schema information for a PostgreSQL table
    
    Args:
        table_name: Name of the table
        schema_name: Name of the schema (default: public)
    
    Returns:
        List of dictionaries containing column information
    """
    # Database connection parameters from .env
    conn_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'testdb'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
    }
    
    # SQL query to get table schema information
    schema_query = """
    SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        ordinal_position
    FROM information_schema.columns 
    WHERE table_name = %s 
        AND table_schema = %s
    ORDER BY ordinal_position;
    """
    
    try:
        # Connect to PostgreSQL
        # print(f"Connecting to PostgreSQL at {conn_params['host']}:{conn_params['port']}")
        # print(f"Database: {conn_params['database']}")
        
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Execute the schema query
        cursor.execute(schema_query, (table_name, schema_name))
        columns = cursor.fetchall()
        
        if not columns:
            print(f"Table '{schema_name}.{table_name}' not found or has no columns")
            return []
        
        # Format the results
        schema_info = []
        for col in columns:
            column_info = {
                'column_name': col[0],
                'data_type': col[1],
                'is_nullable': col[2],
                'column_default': col[3],
                'character_maximum_length': col[4],
                'numeric_precision': col[5],
                'numeric_scale': col[6],
                'ordinal_position': col[7]
            }
            schema_info.append(column_info)
        

        
        for col in schema_info:
            column_name = col['column_name']
            data_type = col['data_type']
            if col['character_maximum_length']:
                data_type += f"({col['character_maximum_length']})"
            elif col['numeric_precision'] and col['numeric_scale']:
                data_type += f"({col['numeric_precision']},{col['numeric_scale']})"
            elif col['numeric_precision']:
                data_type += f"({col['numeric_precision']})"
            
            nullable = "YES" if col['is_nullable'] == 'YES' else "NO"
            default = str(col['column_default']) if col['column_default'] else "NULL"
            
            # print(f"{column_name:<20} {data_type:<25} {nullable:<10} {default:<15}")
        
        cursor.close()
        conn.close()
        
        # print(f"\nTotal columns: {len(schema_info)}")
        return schema_info
        
    except psycopg2.Error as e:
        print(f"PostgreSQL Error: {e}")
        return []
    except Exception as e:
        print(f"Error: {e}")
        return []

def map_postgres_to_pyarrow_type(col_info):
    """
    Map PostgreSQL data types to PyArrow types based on column information
    """
    data_type = col_info['data_type'].lower()
    precision = col_info['numeric_precision']
    scale = col_info['numeric_scale']
    
    # Integer types
    if data_type in ['smallint', 'int2']:
        return pa.int16()
    elif data_type in ['integer', 'int4', 'serial']:
        return pa.int32()
    elif data_type in ['bigint', 'int8', 'bigserial']:
        return pa.int64()
    
    # Decimal/Numeric types
    elif data_type in ['numeric', 'decimal']:
        if precision and scale is not None:
            # Use decimal128 for most cases, decimal256 for very high precision
            if precision <= 38:
                return pa.decimal128(precision, scale)
            else:
                return pa.decimal256(precision, scale)
        else:
            # For numeric without precision/scale, use string or float64
            return pa.float64()  # or pa.string() for exact precision
    
    # Floating point types
    elif data_type in ['real', 'float4']:
        return pa.float32()
    elif data_type in ['double precision', 'float8']:
        return pa.float64()
    
    # Money type (PostgreSQL specific)
    elif data_type == 'money':
        return pa.decimal128(19, 2)  # Common representation
    
    # Boolean
    elif data_type == 'boolean':
        return pa.bool_()
    
    # String types
    elif data_type in ['character varying', 'varchar', 'character', 'char', 'text']:
        return pa.string()
    
    # Date/Time types
    elif data_type == 'date':
        return pa.date32()
    elif data_type == 'time':
        return pa.time64('us')
    elif data_type in ['timestamp', 'timestamp without time zone']:
        return pa.timestamp('us')
    elif data_type in ['timestamp with time zone', 'timestamptz']:
        return pa.timestamp('us', tz='UTC')
    
    # Binary types
    elif data_type == 'bytea':
        return pa.binary()
    
    # UUID
    elif data_type == 'uuid':
        return pa.string()  # or pa.binary(16) for binary representation
    
    # JSON types
    elif data_type in ['json', 'jsonb']:
        return pa.string()
    
    # Array types (basic handling)
    elif data_type.startswith('ARRAY') or data_type.endswith('[]'):
        # For arrays, you'd need more complex logic
        return pa.string()  # Simplified - store as string
    
    # Default fallback
    else:
        return pa.string()

# Example usage with your existing schema retrieval
def create_pyarrow_schema_from_postgres(table_name="users", schema_name="public"):
    """
    Create a PyArrow schema from PostgreSQL table schema
    """
    postgres_schema = get_postgres_schema(table_name, schema_name)
    
    if not postgres_schema:
        return None
    
    arrow_fields = []
    for col in postgres_schema:
        arrow_type = map_postgres_to_pyarrow_type(col)
        nullable = col['is_nullable'] == 'YES'
        arrow_fields.append(pa.field(col['column_name'], arrow_type, nullable=nullable))

    arrow_fields.append(pa.field('__rowMarker__', pa.int8(), nullable=True))  # Add row marker field
    return pa.schema(arrow_fields)

def row_marker(value):
    if value in ["r", "c"]:
        return 4
    elif value == "u":
        return 4
    elif value == "d":
        return 2
    
def transform_message(message):

    mes = message["after"] if message["after"] is not None else message["before"]
    mes["timestamp"] = message["source"]["ts_ms"]
    mes["__rowMarker__"] = row_marker(message["op"])
    return mes

def clean_dataframe_for_pyarrow(df, schema):
    """
    Clean DataFrame to match PyArrow schema expectations
    """
    df_cleaned = df.copy()
    
    for field in schema:
        col_name = field.name
        if col_name in df_cleaned.columns:
            if pa.types.is_decimal(field.type):
                # Convert to Decimal for decimal columns
                df_cleaned[col_name] = df_cleaned[col_name].apply(
                    lambda x: Decimal(str(x)) if x is not None and x != '' else None
                )
            elif pa.types.is_integer(field.type):
                # Convert to int for integer columns
                df_cleaned[col_name] = pd.to_numeric(df_cleaned[col_name], errors='coerce').astype('Int64')
            elif pa.types.is_floating(field.type):
                # Convert to float for floating point columns
                df_cleaned[col_name] = pd.to_numeric(df_cleaned[col_name], errors='coerce')
            elif pa.types.is_string(field.type):
                # Ensure string columns are strings
                df_cleaned[col_name] = df_cleaned[col_name].astype(str)
    
    return df_cleaned

def get_parquet(postgres_db, table_name, client_id=None, group_id=None, max_messages=10000, timeout_ms=5000):
    topic_name = f"{postgres_db}.{table_name}"
    schema_name = table_name.split(".")[0] if "." in table_name else "public"
    if "." in table_name:
        table_name = table_name.split(".")[1]
    schema = create_pyarrow_schema_from_postgres(table_name, schema_name)
    messages = list_messages(topic_name, client_id=client_id, group_id=group_id, max_messages=max_messages, timeout_ms=timeout_ms)

    messages_transformed = [transform_message(mes) for mes in messages if "after" in mes]
    df_concat = pd.DataFrame(messages_transformed)
    df_concat = clean_dataframe_for_pyarrow(df_concat, schema)
    if df_concat.empty:
        print("No messages found in the topic")
    else:
        max_timestamp_indices = df_concat.groupby("id")["timestamp"].idxmax()
        df_concat = df_concat.loc[max_timestamp_indices]
        df_concat.drop(columns=["timestamp"], inplace=True, errors='ignore')
    return df_concat, schema