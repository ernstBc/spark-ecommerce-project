# ETL Process 
# Extract Data from Kaggle and put it in local machine or HDFS


import os
import shutil
import kagglehub
from src.data_ingestion.download_data import download_dataset


if __name__ =='__main__':
    download_dataset()

    
    pass