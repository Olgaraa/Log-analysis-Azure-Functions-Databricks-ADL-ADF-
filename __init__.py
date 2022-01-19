import datetime
import logging
import azure.functions as func
from bs4 import BeautifulSoup
import requests
import gzip
import shutil

def main(mytimer: func.TimerRequest) -> None:

try:
    html_page = requests.get('https://www.secrepo.com/self.logs/')
    html_page.raise_for_status()
    soup = BeautifulSoup(html_page.text, 'html.parser')
    soup=str(soup)
    
except Exception as e:
    print(e)

#matching the files that were modified today    
date_now=datetime.datetime.now().date()
pattern = re.compile("<a.href.*log.*</a>."+str(date_now)+".*K")
match=re.findall(pattern,soup)
match=str(match)
pattern2=re.compile("[a-z]*.log.....-..-...gz")
match2=re.findall(pattern2,match)

#removing duplicates
match2=list(dict.fromkeys(match2))

#removing the ".gz"
file_name=[i.strip('.gz') for i in match2]

#downloading and unzipping the files
for url in match2:
    with gzip.open(urlopen("https://www.secrepo.com/self.logs/"+url), 'rb') as f_in:
        for name in file_name:
            with open(name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
