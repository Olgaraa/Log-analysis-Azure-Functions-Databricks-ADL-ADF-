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
from azure.storage.blob import ContainerClient

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

    container = ContainerClient.from_connection_string(conn_str=adls_conn_string, container_name="logs")
    blob_list = container.list_blobs()


    try:
        html_page = requests.get('https://www.secrepo.com/self.logs/')
        html_page.raise_for_status()
        
    except Exception as e:
        print("An error has occured", e)

    #creating a list containing all the file names that are currently on the website
    soup = BeautifulSoup(html_page.text, 'html.parser')
    soup=str(soup)  
    match=re.findall("(access.log.[\d-]+.gz).*(\d{4}-\d{2}-\d{2})",soup)
    match=list(sum(match, ()))
    file_name=[]
    for i in range(0, len(match)//2 - 1): file_name.append(match[2*i]+"_"+match[2*i+1])

    number_of_elements = len(file_name)
    print("Total number of access logs on the website:", number_of_elements)

    #creating a list containing all the file names that are currently on ADLS
    adls_files=[]
    for x in blob_list:
        adls_files.append(x)
    adls_file_names = [ value['name'] for value in adls_files]

    number_of_elements2 = len(adls_file_names)
    print("Total number of access logs on ADLS:", number_of_elements2)
   
    #checking the difference between the list with the file_names from the website and the list with the file_names from ADLS
    difference=[x for x in file_name if x not in adls_file_names]

    number_of_elements3 = len(difference)
    print("Number of access logs that will be downloaded and uploaded (difference):", number_of_elements3)

    #taking the name of the files without the date, in order to create a url
    str_diff=str(difference)
    links=re.findall("access.log.....-..-...gz",str_diff)

    #downloading and unzipping the files that have not been download yet
    filename_position=0
    for url in links:
        with gzip.open(urlopen("https://www.secrepo.com/self.logs/"+url), 'rb') as f_in:
            with open(difference[filename_position], 'wb'):
                data=f_in.read()
                #uploading the files to ADLS
                write_file_to_adls(
                datalake_service_client = adls_service_client,
                filesystem_name = "logs",
                file_path = difference[filename_position],
                file_data = data)
                filename_position=filename_position+1

    return func.HttpResponse(status_code=200)
