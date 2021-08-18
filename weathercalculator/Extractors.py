from datetime import datetime
from os import path
from pathlib import Path

import requests

api_url = "https://api.dataplatform.knmi.nl/open-data"
api_version = "v1"
raw_data_path = "../../data/raw_data"


def get_data():

    # temporary public token
    api_key = "eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6ImNjOWE2YjM3ZjVhODQwMDZiMWIzZGIzZDRjYzVjODFiIiwiaCI6Im11cm11cjEyOCJ9"
    dataset_name = "Actuele10mindataKNMIstations"
    dataset_version = "1"
    max_keys = "10"

    # Use list files request to request first 10 files of the day.
    timestamp = datetime.utcnow().date().strftime("%Y%m%d")
    start_after_filename_prefix = f"KMDS__OPER_P___10M_OBS_L2_{timestamp}"
    list_files_response = requests.get(
        f"{api_url}/{api_version}/datasets/{dataset_name}/versions/{dataset_version}/files",
        headers={"Authorization": api_key},
        params={"maxKeys": max_keys, "startAfterFilename": start_after_filename_prefix},
    )

    list_files = list_files_response.json()
    dataset_files = list_files.get("files")

    # Retrieve first file in the list files response
    filename = dataset_files[0].get("filename")
    endpoint = f"{api_url}/{api_version}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url"
    get_file_response = requests.get(endpoint, headers={"Authorization": api_key})

    if get_file_response.status_code != 200:
        return

    download_url = get_file_response.json().get("temporaryDownloadUrl")
    dataset_file_response = requests.get(download_url)
    if dataset_file_response.status_code != 200:
        return

    return dataset_file_response


def write_data(dataset_file_response, filename):

    # Write dataset file to raw data
    abs_dir_path = path.abspath(raw_data_path)
    absolute_file_name = path.join(abs_dir_path, filename)

    p = Path(absolute_file_name)
    p.write_bytes(dataset_file_response.content)
