import requests
from bs4 import BeautifulSoup
#from gensim.summarization.summarizer import summarize
import re
from datetime import datetime as dtt
#cd = 'http://geopoliticsalert.com/\
    #trump-admin-exploiting-khashoggi-disappearance-to-force-saudis-to-buy-american'
base_url = "http://geopoliticsalert.com/"
base_url_data = requests.get(base_url)
base_url_data = BeautifulSoup(base_url_data.text,"lxml")
links = base_url_data.findAll("h4", {"class": "news-title"})
articles = []
for link in links:
    print (link)
    href = link.findAll('a')[0]['href']
    dictt = {}
    sub_link_data = requests.get(href)
    sub_link_data = BeautifulSoup(sub_link_data.text,"lxml")
    time_list = sub_link_data.findAll('a')
    for time in time_list:
        string = time.text
        time = re.findall(r'\w+\s\d+,\s\d+', string)
        if len(time)>0:
            final_time = time[0]
            final_time = dtt.strptime(final_time, "%B %d, %Y")
    title = sub_link_data.title.text
    summary = ""
    contents = sub_link_data.findAll("p")
    for content in contents:
        summary += str(content.text)
    #final_summary = summarize(summary)
    dictt.update({"Website": "Geopoliticsalert.com","Category": "Geopolitics","url": str(href), "headline": str(title), "summary": summary, "published_date": str(final_time)})
    
    articles.append(dictt)
    break
f = open ("geoalert3.txt", 'w')
f.write(str(articles))
f.close()
