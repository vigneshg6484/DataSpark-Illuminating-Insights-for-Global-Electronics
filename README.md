import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
from sqlalchemy import create_engine

customers = pd.read_csv('customers.csv')
products = pd.read_csv('products.csv')
sales = pd.read_csv('sales.csv')
stores = pd.read_csv('stores.csv')
exchange_rates = pd.read_csv('exchange_rates.csv')

customers.fillna(method='ffill', inplace=True)
products.fillna(method='ffill', inplace=True)
sales.fillna(method='ffill', inplace=True)
stores.fillna(method='ffill', inplace=True)
exchange_rates.fillna(method='ffill', inplace=True)

sales['sale_date'] = pd.to_datetime(sales['sale_date'])
customers['birthdate'] = pd.to_datetime(customers['birthdate'])

sales_data = sales.merge(customers, on='customer_id', how='left')
sales_data = sales_data.merge(products, on='product_id', how='left')
sales_data = sales_data.merge(stores, on='store_id', how='left')
sales_data = sales_data.merge(exchange_rates, on='currency', how='left')

conn = sqlite3.connect('global_electronics.db')
sales_data.to_sql('sales_data', conn, if_exists='replace', index=False)
customers.to_sql('customers', conn, if_exists='replace', index=False)
products.to_sql('products', conn, if_exists='replace', index=False)
stores.to_sql('stores', conn, if_exists='replace', index=False)
exchange_rates.to_sql('exchange_rates', conn, if_exists='replace', index=False)

engine = create_engine('sqlite:///global_electronics.db')

query1 = pd.read_sql_query("SELECT gender, COUNT(*) as num_customers FROM customers GROUP BY gender", engine)
query2 = pd.read_sql_query("SELECT city, COUNT(*) as num_customers FROM customers GROUP BY city ORDER BY num_customers DESC", engine)
query3 = pd.read_sql_query("SELECT product_name, SUM(quantity) as total_sold FROM sales_data GROUP BY product_name ORDER BY total_sold DESC", engine)
query4 = pd.read_sql_query("SELECT store_name, SUM(total_amount) as revenue FROM sales_data GROUP BY store_name ORDER BY revenue DESC", engine)
query5 = pd.read_sql_query("SELECT sale_date, SUM(total_amount) as total_sales FROM sales_data GROUP BY sale_date ORDER BY sale_date", engine)
query6 = pd.read_sql_query("SELECT currency, SUM(total_amount * exchange_rate) as total_sales_in_usd FROM sales_data GROUP BY currency", engine)
query7 = pd.read_sql_query("SELECT product_category, SUM(total_amount) as revenue FROM sales_data GROUP BY product_category ORDER BY revenue DESC", engine)
query8 = pd.read_sql_query("SELECT state, COUNT(*) as num_stores FROM stores GROUP BY state", engine)
query9 = pd.read_sql_query("SELECT birthdate, COUNT(*) as num_customers FROM customers GROUP BY birthdate ORDER BY num_customers DESC", engine)
query10 = pd.read_sql_query("SELECT store_name, square_meters, SUM(total_amount) as total_revenue FROM sales_data GROUP BY store_name ORDER BY total_revenue DESC", engine)

customers['age'] = 2024 - customers['birthdate'].dt.year
sns.histplot(customers['age'], kde=True)
plt.title('Age Distribution of Customers')
plt.show()

sales_over_time = sales_data.groupby('sale_date').sum()['total_amount']
sales_over_time.plot(figsize=(10, 6))
plt.title('Total Sales Over Time')
plt.xlabel('Date')
plt.ylabel('Total Sales')
plt.grid(True)
plt.show()

top_products = sales_data.groupby('product_name').sum()['quantity'].sort_values(ascending=False).head(10)
sns.barplot(x=top_products.values, y=top_products.index)
plt.title('Top 10 Selling Products')
plt.show()

top_stores = sales_data.groupby('store_name').sum()['total_amount'].sort_values(ascending=False).head(10)
sns.barplot(x=top_stores.values, y=top_stores.index)
plt.title('Top 10 Stores by Revenue')
plt.show()

sales_data['profit_margin'] = (sales_data['price'] - sales_data['cost']) / sales_data['price']
category_profit_margin = sales_data.groupby('product_category').mean()['profit_margin'].sort_values(ascending=False)
sns.barplot(x=category_profit_margin.values, y=category_profit_margin.index)
plt.title('Profit Margin by Product Category')
plt.show()
