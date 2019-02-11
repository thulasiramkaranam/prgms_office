import requests
import re
import datetime
from datetime import datetime as dtt

from bs4 import BeautifulSoup
url = "https://edition.cnn.com/health"
abc = requests.get(url)

url_data = BeautifulSoup(abc.text,"lxml")
data = url_data.findAll("h3", {"class": "cd__headline"})
pattern = r"(\d+/\d+/\d+)"
for record in data:
    
    j = record.findAll("a")
    sub_url = j[0]["href"]
    if '/health/' in sub_url:
        sub_url = "https://edition.cnn.com"+sub_url
        output = re.search(pattern,sub_url)
        date = dtt.strptime(output.group(0), "%Y/%m/%d")
        date_now = dtt.now()
        if (date_now - date).days <60:
            sub_data = requests.get(sub_url)
            url_data = BeautifulSoup(sub_data.text,"lxml")
            title = url_data.contents[0]
            data = url_data.findAll("li", {"class": "el__storyhighlights__item el__storyhighlights--normal"})
            summary3 = ""
            for ii in data:
                summary3 += ii.contents[0]
                summary3 += " "
            if len(cc) >0:
                dictt3 = {}
                dictt3.update({"Website": "The CNN Health","Category": "Disruptive","url": sub_url, "headline": str(title), "summary": str(summary3), "published_date": str(data)})
                
            
        

