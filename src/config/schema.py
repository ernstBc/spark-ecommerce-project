from pyspark.sql.types import (IntegerType,
                               TimestampType,
                               FloatType,
                               StringType,
                               StructType, 
                               StructField,)


CUSTOMER_DATASET_SCHEMA = StructType([
    StructField('customer_id', StringType(), False),
    StructField('customer_unique_id', StringType(), True),
    StructField('customer_zip_code_prefix', StringType(), True),
    StructField('customer_city', StringType(), True),
    StructField('customer_state', StringType(), True),
])


GEOLOCATION_DATASET_SCHEMA = StructType([
    StructField('geolocation_zip_code_prefix', StringType(), True),
    StructField('geolocation_lat', FloatType(), True),
    StructField('geolocation_lng', FloatType(), True),
    StructField('geolocation_city', StringType(), True),
    StructField('geolocation_state', StringType(), True),
])


ORDER_ITEMS_DATASET_SCHEMA = StructType([
    StructField('order_id', StringType(), False),
    StructField('order_item_id', StringType(), True),
    StructField('product_id', StringType(), True),
    StructField('seller_id', StringType(), True),
    StructField('shipping_limit_date', TimestampType(), True),
    StructField('price', FloatType(), True),
    StructField('freight_value', FloatType(), True)
])


PAYMENTS_DATASET_SCHEMA = StructType([
    StructField('order_id', StringType(), False),
    StructField('payment_sequential', IntegerType(), True),
    StructField('payment_type', StringType(), True),
    StructField('payment_installments', IntegerType(), True),
    StructField('payment_value', FloatType(), True)
])


REVIEWS_DATASET_SCHEMA=StructType([
    StructField('review_id', StringType(), True),
    StructField('order_id', StringType(), True),
    StructField('review_score', FloatType(), True),
    StructField('review_comment_title', StringType(), True),
    StructField('review_comment_message', StringType(), True),
    StructField('review_creation_date', TimestampType(), True),
    StructField('review_answer_timestamp', TimestampType(), True)
])


ORDERS_DATASET_SCHEMA = StructType([
    StructField('order_id', StringType(), False),
    StructField('customer_id', StringType(), True),
    StructField('order_status', StringType(), True),
    StructField('order_purchase_timestamp', TimestampType(), True),
    StructField('order_approved_at', TimestampType(), True),
    StructField('order_delivered_carrier_date', TimestampType(), True),
    StructField('order_delivered_customer_date', TimestampType(), True),
    StructField('order_estimated_delivery_date', TimestampType(), True)
])


PRODUCTS_DATASET_SCHEMA = StructType([
    StructField('product_id', StringType(), True),
    StructField('product_category_name', StringType(), True),
    StructField('product_name_lenght', IntegerType(), True),
    StructField('product_description_lenght', IntegerType(), True),
    StructField('product_photos_qty', IntegerType(), True),
    StructField('product_weight_g', IntegerType(), True),
    StructField('product_length_cm', IntegerType(), True),
    StructField('product_height_cm', IntegerType(), True),
    StructField('product_width_cm', IntegerType(), True)
])


SELLERS_DATASET_SCHEMA = StructType([
    StructField('seller_id', StringType(), True),
    StructField('seller_zip_code_prefix', StringType(), True),
    StructField('seller_city', StringType(), True),
    StructField('seller_state', StringType(), True)
])


TRANSLATIONS_DATASET_SCHEMA=StructType([
    StructField('product_category_name', StringType(), True),
    StructField('product_category_name_english', StringType(), True)
])
