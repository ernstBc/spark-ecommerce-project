import kagglehub
import os
import shutil 
from src.config.config import DATA_FOLDER, DATASET_URL
from src.utils import create_directory


def download_dataset(dataset = DATASET_URL, download_path = DATA_FOLDER):
    if not os.path.exists(DATA_FOLDER):
        create_directory(download_path)

    path = kagglehub.dataset_download(dataset, force_download=True)

    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        shutil.move(file_path, download_path)


if __name__ == '__main__':
    download_dataset()


