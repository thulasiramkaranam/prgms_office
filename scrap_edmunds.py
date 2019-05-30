import requests
from bs4 import BeautifulSoup
#from gensim.summarization.summarizer import summarize
import re
from datetime import datetime as dtt
global articles
articles = []
def scrap_forums():

    for i in range(1,2,1):
        base_url = "https://forums.edmunds.com/discussions/tagged/x/repairs-maintenance/p{}"
        base_url = base_url.format(i)
        print(base_url)
        base_url_data = requests.get(base_url)
        base_url_data = BeautifulSoup(base_url_data.text,"lxml")

        links = base_url_data.findAll("ul", {"class": "DataList Discussions"})
        print(type(links))

        links = links[0]
        articles = []

        for link in links:
            if link != '\n':
                
                carname = link.findAll('a')[2].text
                url = link.findAll('a')[0]['href']
                no_views = link.findAll('span', {'class': 'MItem MCount ViewCount'})[0].text
                no_comments = link.findAll('span', {'class': 'MItem MCount CommentCount'})[0].text
                sub_link_data = requests.get(url)
                sub_link_data = BeautifulSoup(sub_link_data.text,"lxml")
                headline = sub_link_data.title.text
                messages = sub_link_data.findAll('div', {'class': 'Message userContent'})
                messages_string = str(carname) + ' | '+ str(no_views) + ' | '+ str(no_comments) + ' | '
                for message in messages:
                    messages_string += message.contents[0]
                    p_tag = message.findAll('p')
                    if len(p_tag) > 0:
                        for i in range(len(p_tag)):

                            messages_string += p_tag[i].text
                time = dtt.now()
                dictt = {}
                dictt.update({"Website": "https://forums.edmunds.com/discussions/tagged/x/repairs-maintenance", "Category": "retail", "url": str(url),
                                "headline": str(headline), "summary": str(messages_string), "published_date": time, "tags": ' ' })
                articles.append(dictt)
    print(articles)
    print(len(articles))
    print("after articles")
    #return articles
scrap_forums()
            




    
    
