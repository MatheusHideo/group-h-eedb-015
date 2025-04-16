import kagglehub
import os
import json
import boto3
import shutil



def download_files(download_path:str):
    files = [
    "raw/2016/Plan_Attributes_PUF_2015-12-08.csv",
    "raw/2016/Benefits_Cost_Sharing_PUF_2015-12-08.csv",
    "raw/2016/Network_PUF_2015-12-08.csv",
    "raw/2016/Rate_PUF_2015-12-08.csv",
    "raw/2016/ServiceArea_PUF_2015-12-08.csv",
    "raw/2016/Business_Rules_PUF_2015-12-08.csv",
    "raw/2015/Network_PUF.csv",
    "raw/2015/Plan_Crosswalk_PUF_2014-12-22.csv",
    "raw/2015/Plan_Attributes_PUF.csv",
    "raw/2015/Service_Area_PUF.csv",
    "raw/2015/Rate_PUF.csv",
    "raw/2015/Benefits_Cost_Sharing_PUF.csv",
    "raw/2015/Business_Rules_PUF_Reformat.csv",
    "raw/2014/Network_PUF.csv",
    "raw/2014/Service_Area_PUF.csv",
    "raw/2014/Plan_Attributes_PUF_2014_2015-03-09.csv",
    "raw/2014/Rate_PUF.csv",
    "raw/2014/Benefits_Cost_Sharing_PUF.csv",
    "raw/2014/Business_Rules_PUF.csv",
    "raw/2016/Plan_ID_Crosswalk_PUF_2015-12-07.CSV"
    ]
    
    os.environ['KAGGLEHUB_CACHE'] = download_path
    error_files = []
    files_downloaded = []
    
    for file in files:
        try:
            path2 = kagglehub.dataset_download("hhs/health-insurance-marketplace", path=file)
            print(path2)
            files_downloaded.append(path2)
        except Exception as err:
            error_files.append(err)
    return {
        "error": error_files,
        "succeed": files_downloaded
    }

def find_and_upload(local_path: str, bucket_name:str, bucket_folder: str):
    s3 = boto3.client("s3")
    
    s3.upload_file(local_path, bucket_name, bucket_folder)


def create_zip_file(path: str, zip_file_path: str):
    shutil.make_archive(zip_file_path, 'zip', path)

def handler(event, context):
    
    bucket_name = "landing-test-edb"
    bucket_folder = "health-insurance-marketplace"
    
    download_path = "/tmp/kaggle"
    zip_file_path = "/tmp/health-insurance-marketplace"
    output_path = f"{download_path}/datasets/hhs/health-insurance-marketplace/versions/2"
    error, succeed  = download_files(download_path)
    
    create_zip_file(output_path, zip_file_path)

    find_and_upload(f'{zip_file_path}.zip', bucket_name, bucket_folder)
    


    if len(error) > 1:
        return {
        "statusCode": 400,
        "body": json.dumps(error)
        }
    return {
        "statusCode": 200,
        "body": json.dumps(succeed)
    }

handler(None, None)