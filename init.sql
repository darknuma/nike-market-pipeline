-- Create products table
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create sales table
CREATE TABLE IF NOT EXISTS sales (
    sale_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    sale_date TIMESTAMP NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_product_id ON sales(product_id);

-- Insert some initial sample data
INSERT INTO products (name, category, price, stock_quantity) VALUES
    ('Air Max 270', 'Running', 149.99, 50),
    ('Air Jordan 1', 'Basketball', 199.99, 30),
    ('Nike Free Run', 'Running', 89.99, 75),
    ('Nike Dri-FIT Training', 'Training', 34.99, 100),
    ('Nike Air Force 1', 'Lifestyle', 99.99, 45);

-- Insert some initial sales data
INSERT INTO sales (product_id, quantity, sale_date, total_amount) VALUES
    (1, 2, CURRENT_TIMESTAMP - INTERVAL '5 days', 299.98),
    (2, 1, CURRENT_TIMESTAMP - INTERVAL '3 days', 199.99),
    (3, 3, CURRENT_TIMESTAMP - INTERVAL '1 day', 269.97),
    (4, 5, CURRENT_TIMESTAMP - INTERVAL '2 days', 174.95),
    (5, 2, CURRENT_TIMESTAMP - INTERVAL '4 days', 199.98); 