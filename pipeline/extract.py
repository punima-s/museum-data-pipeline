"""Extract files from S3"""
import os
from dotenv import dotenv_values
from boto3 import client
import csv
from argparse import ArgumentParser
import logging


def get_config():
    return dotenv_values()


def get_client(config) -> 'client.S3':
    """Getting the s3 client"""
    return client('s3',
                  aws_access_key_id=config.get("ACCESS_KEY"),
                  aws_secret_access_key=config.get("SECRET_KEY"))


def get_bucket_names(client: 'client.S3'):
    """Gets all the s3 buckets"""
    return [buckets['Name'] for buckets in client.list_buckets()['Buckets']]


def is_bucket_valid(bucket_name: str, buckets: list) -> None:
    """Raise error if bucket name is not in the like of buckets in s3."""
    if bucket_name not in buckets:
        raise NameError


def get_objects(client: 'client.S3', bucket: str) -> list[dict]:
    """Get the objects inside Bucket"""
    return client.list_objects(Bucket=bucket)['Contents']


def download_hist_files(client: 'client.S3', bucket: str, object_list: list) -> list[str]:
    """Download hist_data files into local file folder 'lmnh_hist'"""
    for file in object_list:
        if os.path.exists(f"./lmnh_hist/{file['Key']}"):
            continue
        if 'lmnh_hist_data_' in file['Key'] and '.csv' in file['Key']:
            client.download_file(
                Bucket=bucket, Key=file['Key'], Filename=f"./lmnh_hist/{file['Key']}")


def download_exhibit_files(client: 'client.S3', bucket: str,
                           object_list: list) -> None:
    """Download hist_data files into local file folder 'lmnh_exhibitions'"""
    for file in object_list:
        if os.path.exists(f"./lmnh_exhibitions/{file['Key']}"):
            continue
        if 'lmnh_exhibition_' in file['Key'] and '.json' in file['Key']:
            client.download_file(
                Bucket=bucket, Key=file['Key'], Filename=f"./lmnh_exhibitions/{file['Key']}")


def clear_folder(file_path: str) -> None:
    """Clearing files under a folder"""
    files = os.listdir(file_path)

    for file in files:
        os.remove(f"{file_path}/{file}")


def create_directory(folder_path: str) -> None:
    """Creates a new folder if it doesn't exist or else it clears the files inside the folder."""
    os.makedirs(folder_path, exist_ok=True)


def combining_csv_files(folder_path: str) -> None:
    """Combining csv files together"""
    csv_files = os.listdir(folder_path)
    data = []
    for file in csv_files:
        if file == "lmnh_hist_data_merged.csv":
            continue
        file_path = os.path.join(folder_path, file)
        with open(file_path) as csv_file:
            csv_reader = csv.reader(csv_file)
            fields = csv_reader.__next__()
            for row in csv_reader:
                data.append(row)

    clear_folder(folder_path)

    with open(f"{folder_path}/lmnh_hist_data_merged.csv", "w") as file:
        w = csv.writer(file)
        w.writerow(fields)
        for row in data:
            w.writerow(row)


def main(hist_filepath: str, exhibition_filepath: str, bucket_name: str, s3_client: 'client.S3') -> None:
    """Running main functions from here."""
    is_bucket_valid(bucket_name, get_bucket_names(s3_client))

    # Making a directory with no files
    create_directory(hist_filepath)
    create_directory(exhibition_filepath)

    # Store the list of all objects in the bucket in objects
    objects = get_objects(s3_client, bucket_name)

    # Download files
    download_hist_files(s3_client, bucket_name, objects)
    download_exhibit_files(s3_client, bucket_name, objects)

    # Combine csv files
    combining_csv_files(hist_filepath)


if __name__ == "__main__":

    bucket_name = 'sigma-resources-museum'

    config = get_config()

    hist_filepath = config.get("HIST_FOLDER_PATH")
    exhibition_filepath = config.get("EXHIBITION_FOLDER_PATH")
    s3_client = get_client(config)

    try:
        main(hist_filepath, exhibition_filepath, bucket_name, s3_client)
    except NameError as e:
        logging.exception("Invalid Bucket name.")
