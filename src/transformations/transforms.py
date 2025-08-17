from pyspark.sql.functions import col, length, mean, mode, expr, when, udf
from pyspark.sql.types import FloatType
from pyspark.ml.feature import Imputer
import math


def review_dataset_clean(df):
    """
    Clean Review dataset by removing null rows, duplicates and corrupt data
    """
    id_len = 32
    df = df\
            .filter((col('order_id').isNotNull())) \
            .filter(length(col('order_id')) == id_len) \
            .filter(~ col('order_id').contains(' ')) \
            .dropDuplicates()
    
    return df


def geolocation_dataset_transform(df):
    """
    Transform geolocation dataset by calculate approximate coordinates
    by zipcode prefix. 
    """
    # This step is needed before doing any join because the dataset was annonymized 
    #removing any id to join besides zipcode.

    df = df\
        .groupBy('geolocation_zip_code_prefix') \
        .agg(
            mean('geolocation_lat').alias('geo_lat_mean'),
            mean('geolocation_lng').alias('geo_lng_mean'),
            mode('geolocation_city').alias('geolocation_city'),
            mode('geolocation_state').alias('geolocation_state')
        )
    
    return df


def products_dataset_clean(df):    
    """
    Clean products dataset by imputing null values 
    """
    input_prod_cols = ['product_weight_g',
                       'product_length_cm', 
                       'product_height_cm', 
                       'product_width_cm']
    output_prod_cols = [column +'_imp' for column in input_prod_cols]

    imputer = Imputer(strategy='mode',
                    inputCols=input_prod_cols, 
                    outputCols= output_prod_cols)

    df = imputer.fit(df).transform(df)

    for col_old, col_new in zip(input_prod_cols, output_prod_cols):
        df=df.drop(col_old)
        df=df.withColumnRenamed(col_new, col_old)

    return df


def full_orders_dataset_transform(df):
    # get day of week (weekday or weekend)
    df = df.withColumn('weekday', 
                when(expr("dayofweek(order_purchase_timestamp) in (1,7)"), 'weekend')
                .otherwise('weekday'))

    # # calculate distance
    # haversine_udf_km =udf(lambda lat1, lon1, lat2, lon2: haversine_distance(lat1, lon1, lat2, lon2, unit='km'), FloatType())

    # df = df.withColumn('aprox_distance_customer_seller', 
    #                    haversine_udf_km('seller_geo_lat_mean', 
    #                                       'seller_geo_mean_lng', 
    #                                       'customer_geo_lat_mean', 
    #                                       'customer_geo_mean_lng'))

    return df


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

