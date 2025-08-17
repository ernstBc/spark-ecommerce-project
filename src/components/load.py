import os
from typing import Dict, Literal
from src.config.config import DATA_PROCESSED_FOLDER



class Load:
    def __init__(self, spark_session, 
                 mode:Literal['local', 'postgres'] = 'local', 
                 pg_config:Dict = None):
        self.spark = spark_session
        self.mode = mode

        if mode =='postgres':
            self.config = pg_config


    def load(self, df, df_name:str):

        if self.mode == 'local':
            df.write\
                .option('compression', 'gzip')\
                .mode('overwrite')\
                .parquet(os.path.join(DATA_PROCESSED_FOLDER, df_name + '.parquet'))
        
        else:
            df.write.jdbc(
                url = self.config['url'],
                table=df_name,
                mode='overwrite',
                properties=self.config['connection_properties']
            )