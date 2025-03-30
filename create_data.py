import psycopg2
from psycopg2.extras import execute_values
import random
from datetime import datetime, timedelta

# Database connection parameters
DB_PARAMS = {
    'dbname': 'nike_db',
    'user': 'nike_user',
    'password': 'nike_password',
    'host': 'localhost',
    'port': '5432'
}

def create_tables(conn):
    with conn.cursor() as cur:
        # Create products table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                category VARCHAR(50) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                stock_quantity INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create sales table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                sale_id SERIAL PRIMARY KEY,
                product_id INTEGER REFERENCES products(product_id),
                quantity INTEGER NOT NULL,
                sale_date TIMESTAMP NOT NULL,
                total_amount DECIMAL(10,2) NOT NULL
            )
        """)
    conn.commit()

def generate_sample_data():
    # Sample product categories
    categories = ['Running', 'Basketball', 'Training', 'Lifestyle', 'Football']
    
    # Sample product names
    product_names = [
        'Air Max', 'Zoom', 'Free Run', 'Air Jordan', 'Dri-FIT',
        'React', 'Air Force', 'Blazer', 'Cortez', 'Dunk'
    ]
    
    products = []
    for i in range(20):  # Generate 20 products
        name = f"{random.choice(product_names)} {random.randint(1, 1000)}"
        category = random.choice(categories)
        price = round(random.uniform(50, 300), 2)
        stock = random.randint(10, 100)
        products.append((name, category, price, stock))
    
    return products

def insert_sample_data(conn):
    products = generate_sample_data()
    
    with conn.cursor() as cur:
        # Insert products
        execute_values(cur, """
            INSERT INTO products (name, category, price, stock_quantity)
            VALUES %s
            RETURNING product_id
        """, products)
        
        product_ids = [row[0] for row in cur.fetchall()]
        
        # Generate and insert sales data
        sales = []
        for _ in range(50):  # Generate 50 sales
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 5)
            sale_date = datetime.now() - timedelta(days=random.randint(0, 30))
            
            # Get product price
            cur.execute("SELECT price FROM products WHERE product_id = %s", (product_id,))
            price = cur.fetchone()[0]
            total_amount = price * quantity
            
            sales.append((product_id, quantity, sale_date, total_amount))
        
        execute_values(cur, """
            INSERT INTO sales (product_id, quantity, sale_date, total_amount)
            VALUES %s
        """, sales)
    
    conn.commit()

def main():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("Connected to PostgreSQL database")
        
        create_tables(conn)
        print("Created tables successfully")
        
        insert_sample_data(conn)
        print("Inserted sample data successfully")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    main() 