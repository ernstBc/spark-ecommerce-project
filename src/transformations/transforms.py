from pyspark.sql.functions import col, length, mean, mode, avg
from pyspark.ml.feature import Imputer


def review_dataset_clean(df):
    """
    Clean Review dataset by removing null rows, duplicates and corrupt data
    """
    id_len = 32
    df = df\
            .filter((col('order_id').isNotNull()))\
            .filter((length(col('review_id')) == id_len)) \
            .filter(length(col('order_id')) == id_len) \
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