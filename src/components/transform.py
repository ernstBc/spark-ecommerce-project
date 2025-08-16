import os
import math
from pyspark.sql.functions import (col,
                                  length,
                                  broadcast)
from src.config.config import (CUSTOMER_DATASET,
                               GEOLOCATION_DATASET,
                               ORDER_DATASET,
                               ORDER_ITEMS_DATASET,
                               ORDER_REVIEWS_DATASET,
                               ORDER_PAYMENTS_DATASET,
                               PRODUCTS_DATASET,
                               SELLERS_DATASET,
                               PRODUCT_TRANSLATIONS)
from src.config.schema import (CUSTOMER_DATASET_SCHEMA,
                               GEOLOCATION_DATASET_SCHEMA,
                               ORDERS_DATASET_SCHEMA,
                               ORDER_ITEMS_DATASET_SCHEMA,
                               REVIEWS_DATASET_SCHEMA,
                               PAYMENTS_DATASET_SCHEMA,
                               PRODUCTS_DATASET_SCHEMA,
                               SELLERS_DATASET_SCHEMA,
                               TRANSLATIONS_DATASET_SCHEMA)
from src.transformations.transforms import (geolocation_dataset_transform, 
                                            review_dataset_clean,
                                            products_dataset_clean)


class Transform:
    def __init__(self, spark_session):
        self.spark = spark_session
    

    def transform(self):
        spark = self.spark

        # load all datasets
        customer_dataset, \
            order_dataset,\
                items_dataset,\
                    payments_dataset,\
                        reviews_dataset,\
                            geolocation_dataset,\
                                products_dataset,\
                                    sellers_dataset,\
                                        translations_dataset = self.load_datasets()

        # clean datasets which needs dataset cleaning 

        # reviews
        # drop row without order_id, corrupt data and duplicates
        reviews_dataset = review_dataset_clean(reviews_dataset)

        # geolocation
        ## recompute lat and lon coordinates 
        geolocation_dataset = geolocation_dataset_transform(geolocation_dataset)
        geolocation_dataset = broadcast(geolocation_dataset)

        # products
        ## impute numerical null features 
        products_dataset = products_dataset_clean(products_dataset)



        # joins
        # join orders with customers
        orders_customers= order_dataset.join(customer_dataset, 'customer_id', 'inner')
        # joing with items
        orders_customers_items_df = orders_customers.join(items_dataset, 'order_id', 'inner')
        # join sellers
        orders_customer_items_sellers = orders_customers_items_df.join(broadcast(sellers_dataset),
                                                                       'seller_id',
                                                                       'inner')
        # join products
        order_products = orders_customer_items_sellers.join(products_dataset,
                                                            'product_id',
                                                            'inner')
        # join customer geolocation 
        geo_cols = geolocation_dataset.columns
        full_orders_geo=order_products.join(geolocation_dataset,
                                            order_products.customer_zip_code_prefix == geolocation_dataset.geolocation_zip_code_prefix,
                                            'left')
        ## rename cols
        for col in geo_cols:
            full_orders_geo = full_orders_geo.withColumnRenamed(existing=col, new='customer_'+col)


        # join seller geolocation
        full_orders_geo=full_orders_geo.join(geolocation_dataset, 
                            full_orders_geo.seller_zip_code_prefix == geolocation_dataset.geolocation_zip_code_prefix, 
                            'left')
        for col in geo_cols:
            full_orders_geo = full_orders_geo.withColumnRenamed(existing=col, new='seller_'+col)

        # join reviews
        full_order_review = full_orders_geo.join(reviews_dataset, 'order_id', 'left')
        #join payments 
        full_orders = full_order_review.join(payments_dataset, 'order_id', 'left')

        
        # remove not important columns
        columns = full_orders.columns
        columns_to_remove =  [
        'customer_geolocation_city',
        'customer_geolocation_zip_code_prefix',
        'seller_geolocation_zip_code_prefix',
        'seller_geolocation_state',
        'seller_geolocation_city']

        for r in columns_to_remove:
            columns.remove(r)

        columns.sort()

        full_orders = full_orders.select(columns)

        # repartion dataset
        full_orders = full_orders.coalesce(4)


        return full_orders
    


    def load_datasets(self):
        spark = self.spark
        customer_dataset = spark.read.csv(CUSTOMER_DATASET, header=True, schema=CUSTOMER_DATASET_SCHEMA)
        order_dataset = spark.read.csv(ORDER_DATASET, header=True, schema=ORDERS_DATASET_SCHEMA)
        items_dataset = spark.read.csv(ORDER_ITEMS_DATASET, header=True, schema=ORDER_ITEMS_DATASET_SCHEMA)
        payments_dataset = spark.read.csv(ORDER_PAYMENTS_DATASET, header=True, schema=PAYMENTS_DATASET_SCHEMA)
        reviews_dataset = spark.read.csv(ORDER_REVIEWS_DATASET, header=True, schema=REVIEWS_DATASET_SCHEMA)
        geolocation_dataset = spark.read.csv(GEOLOCATION_DATASET, header=True, schema=GEOLOCATION_DATASET_SCHEMA)
        products_dataset = spark.read.csv(PRODUCTS_DATASET, header=True, schema=PRODUCTS_DATASET_SCHEMA)
        sellers_dataset = spark.read.csv(SELLERS_DATASET, header=True, schema=SELLERS_DATASET_SCHEMA)
        translations_dataset = spark.read.csv(PRODUCT_TRANSLATIONS, header=True, schema=TRANSLATIONS_DATASET_SCHEMA)


        return (
            customer_dataset,
            order_dataset,
            items_dataset,
            payments_dataset,
            reviews_dataset,
            geolocation_dataset,
            products_dataset,
            sellers_dataset,
            translations_dataset
        )
    




# custom trasnformations
def haversine_distance(lat1, lon1, lat2, lon2, unit='km'):
    """
    Calculates the Haversine distance between two points on the Earth
    (specified in decimal degrees).

    The Haversine formula is used to compute the great-circle distance
    between two points on a sphere from their longitudes and latitudes.

    Args:
        lat1 (float): Latitude of the first point.
        lon1 (float): Longitude of the first point.
        lat2 (float): Latitude of the second point.
        lon2 (float): Longitude of the second point.
        unit (str): The desired unit for the output distance. Can be 'km'
                    (kilometers) or 'mi' (miles). Defaults to 'km'.

    Returns:
        float: The distance between the two points in the specified unit.
    """
    # Radius of the Earth in kilometers or miles
    if unit == 'km':
        R = 6371.0
    elif unit == 'mi':
        R = 3958.8
    else:
        raise ValueError("Unit must be 'km' or 'mi'")

    # Convert coordinates from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Calculate the differences in coordinates
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Apply the Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c

    return float(distance)
