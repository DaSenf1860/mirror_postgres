-- Initialize the database with sample tables and data
-- This file will be executed when the PostgreSQL container starts

-- Create a sample users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a sample orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    order_number VARCHAR(100) NOT NULL UNIQUE,
    total_amount DECIMAL(10,2),
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a sample products table
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2),
    category VARCHAR(100),
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
-- INSERT INTO users (username, email, first_name, last_name) VALUES
--     ('john_doe', 'john@example.com', 'John', 'Doe'),
--     ('jane_smith', 'jane@example.com', 'Jane', 'Smith'),
--     ('bob_wilson', 'bob@example.com', 'Bob', 'Wilson');

-- INSERT INTO products (name, description, price, category, stock_quantity) VALUES
--     ('Laptop', 'High-performance laptop', 999.99, 'Electronics', 50),
--     ('Mouse', 'Wireless mouse', 29.99, 'Electronics', 100),
--     ('Keyboard', 'Mechanical keyboard', 79.99, 'Electronics', 75);

-- INSERT INTO orders (user_id, order_number, total_amount, status) VALUES
--     (1, 'ORD-001', 1029.98, 'completed'),
--     (2, 'ORD-002', 79.99, 'pending'),
--     (3, 'ORD-003', 999.99, 'shipped');

-- Create a publication for Debezium (required for logical replication)
CREATE PUBLICATION debezium_publication FOR ALL TABLES;
