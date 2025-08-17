import os
import shutil
import kagglehub
from src.config.config import DATA_FOLDER, DATASET_URL, DATA_PROCESSED_FOLDER
from src.utils import create_directories, create_directory


class Extract:
    def __init__(self, url=DATASET_URL, 
                 path=DATA_FOLDER, 
                 processed_data_dir=DATA_PROCESSED_FOLDER):
        self.url = url
        self.data_path =path
        self.data_processed_path=processed_data_dir


    def extract(self):
        if not os.path.exists(self.data_path):
            print('createcreatecreate')
            create_directories(self.data_path)
            create_directory(self.data_processed_path)
        
        if os.listdir(self.data_path) == []:
            print('nonononono')
            download_data(self.url, self.data_path)


def download_data(url, data_path):
    path = kagglehub.dataset_download(url, force_download=True)
    
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        shutil.move(file_path, data_path)
