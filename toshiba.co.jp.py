
import requests
import datetime
from datetime import datetime as dtt
from bs4 import BeautifulSoup
abc = requests.get('https://www.toshiba.co.jp/about/press/2018_09/index.htm')
url_data = BeautifulSoup(abc.text,"lxml")
links = url_data.findAll('dl', {'class': 'rssGroup'})
for link in links:
    
    href_obj = link.findAll('a', {'class': 'rssItem'})
    href = href_obj[0]['href']
    if 'https' not in href:
        url = 
    print (href)
    print ("After href")
    
