import datetime
import logging
import azure.functions as func
from bs4 import BeautifulSoup
import requests
import gzip
import logging
import azure.functions as func
import os
import re
from urllib.request import urlopen
from azure.storage.filedatalake import DataLakeServiceClient

def write_file_to_adls(datalake_service_client, filesystem_name, file_path, file_data):
    file_client = datalake_service_client.get_file_client(filesystem_name, file_path)
    file_client.create_file()
    file_client.upload_data(data=file_data ,overwrite=True, length=len(file_data))
    file_client.flush_data(len(file_data))
    return True

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    adls_conn_string = os.environ["connect_str"]
    adls_service_client = DataLakeServiceClient.from_connection_string(conn_str = adls_conn_string)

    try:
        html_page = requests.get('https://www.secrepo.com/self.logs/')
        html_page.raise_for_status()
        
    except Exception as e:
        print("An error has occured", e)

    soup = BeautifulSoup(html_page.text, 'html.parser')
    soup=str(soup)

    #matching the files that were modified today    
    date_now=datetime.datetime.now().date()
    pattern = re.compile("<a.href.*log.*</a>."+str(date_now)+".*K")
    match=re.findall(pattern,soup)
    match=str(match)
    print(match)
    pattern2=re.compile("[a-z]*access.log.....-..-...gz")
    match2=re.findall(pattern2,match)
    print(match2)
    #removing duplicates
    match2=list(dict.fromkeys(match2))
    #removing the ".gz"
    file_name=[i.strip('.gz') for i in match2]
    #downloading and unzipping the files
    filename_position=0
    for url in match2:
        with gzip.open(urlopen("https://www.secrepo.com/self.logs/"+url), 'rb') as f_in:
            with open(file_name[filename_position], 'wb'):
                data=f_in.read()
                write_file_to_adls(
                datalake_service_client = adls_service_client,
                filesystem_name = "logs",
                file_path = file_name[filename_position],
                file_data = data)
                filename_position=filename_position+1

    return func.HttpResponse(status_code=200)
