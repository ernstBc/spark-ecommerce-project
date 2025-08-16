import os

DATA_MAIN_DIR='data'
DATASET_URL = "olistbr/brazilian-ecommerce"
DATA_FOLDER = os.path.join(DATA_MAIN_DIR, 'olist')
DATA_PROCESSED_FOLDER=os.path.join(DATA_MAIN_DIR, 'olist_processed')


CUSTOMER_DATASET = os.path.join(DATA_FOLDER, 'olist_customers_dataset.csv')
GEOLOCATION_DATASET = os.path.join(DATA_FOLDER, 'olist_geolocation_dataset.csv')
ORDER_ITEMS_DATASET = os.path.join(DATA_FOLDER, 'olist_order_items_dataset.csv')
ORDER_PAYMENTS_DATASET = os.path.join(DATA_FOLDER, 'olist_order_payments_dataset.csv')
ORDER_REVIEWS_DATASET = os.path.join(DATA_FOLDER, 'olist_order_reviews_dataset.csv')
ORDER_DATASET = os.path.join(DATA_FOLDER, 'olist_orders_dataset.csv')
PRODUCTS_DATASET = os.path.join(DATA_FOLDER, 'olist_products_dataset.csv')
SELLERS_DATASET = os.path.join(DATA_FOLDER, 'olist_sellers_dataset.csv')
PRODUCT_TRANSLATIONS = os.path.join(DATA_FOLDER, 'product_category_name_translation.csv')