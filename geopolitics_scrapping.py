import requests
from bs4 import BeautifulSoup
from datetime import datetime as dtt

counter = 1
booln = True
f = open ('geopolitics_testing.txt', 'w', encoding = 'utf-8')


url = 'https://geopolitics.co/page/'
url += str(counter)+'/'
counter += 1
print (url)
print ("After url")
url_data = requests.get(url)
url_data = BeautifulSoup(url_data.text,"lxml")
links = url_data.findAll("h1", {"class": "entry-title"})
for i in links:
    dictt = {}
    sublink = i.findAll('a')[0]['href']
    print (sublink)
    sublink_data = requests.get(str(sublink))
    sub_url_data = BeautifulSoup(sublink_data.text,"lxml")
    time = sub_url_data.findAll('span', {'class': 'entry-date'})
    date = dtt.strptime(time[0].time.text, "%B %d, %Y")
    content = sub_url_data.findAll("p")
    content = content[0].text
    title = sub_url_data.title.text
    title = title.strip(" | Covert Geopolitics")
    #print (content[0].text)
    #print ("after content")
    print (content)
    print ("After content")
    dictt.update({"Website":"The geopolitics.co","Category": "Geopolitical", "url": sublink, "headline": str(title), "summary": str(content), "published_date": str(date)})
    break
if (dtt.now()-date).days > 90:
    booln = False

