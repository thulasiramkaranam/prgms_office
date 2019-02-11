import datetime
import requests
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import re
from selenium import webdriver
from openpyxl import Workbook
from bs4 import BeautifulSoup
from datetime import datetime as dtt
from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from gensim.summarization.summarizer import summarize
from gensim.summarization import keywords
print ("After importing")
nytimes_data = requests.get("https://www.nytimes.com/section/health")
url_data = BeautifulSoup(nytimes_data.text,"lxml")
data = url_data.findAll("a", {"class": "story-link"})
pattern = r"(\d+/\d+/\d+)"
analyzer= SentimentIntensityAnalyzer()
file_path = r'C:\Users\thulasiram.k\Documents\pgms_office'
def write_into_csv(listt):
    file_name = "Headlines_samsung"+".csv" 
    #print (listt)
    #print ("After listt")
    with open(file_name, "wb") as file_name:

        book = Workbook()
        sheet = book.active
        sheet.title = "News"
        sheet['A1'] = "Category"
        sheet['B1'] = "Datetime"
        sheet['C1'] = "URL"
        sheet['D1'] = "Headlines"
        sheet['E1'] = "Summary"
        sheet['F1'] = "Sentiment"
        sheet['G1'] = "Website"
        counter = 2
        for i in listt:
            article = []
            if type(i["summary"]) == str:
                
                sentiment = analyzer.polarity_scores(i["summary"].strip())
                sentiment = sentiment['compound'] if 'compound' in sentiment else 0
                article.extend([i["Category"], i["published_date"],i["url"], i["headline"].strip(),i["summary"].strip(), sentiment, i["Website"]])
                for m,n in enumerate(article, start = 1):
                    sheet.cell(row=counter, column=m).value = n
                counter += 1
        book.save(file_name)
articles = []

# For Tomshardware.com


# counter = 1
# booln = True
# while booln:    
#     url = 'https://www.tomshardware.com/t/ssd/'
#     url += "page-"+str(counter)+'.html'
#     counter += 1
#     url_data = requests.get(url)
#     url_data = BeautifulSoup(url_data.text,"lxml")
#     links = url_data.findAll('a', {'class': 'item-block-figure'})
#     summary = ""
#     link = 0
#     for i in links:
#         if counter == 1 and link == 0:
#             pass
        
#         else:   
#             dictt = {}
#             sublink = i['href']
#             sublink = 'https://www.tomshardware.com'+sublink
#             print (sublink)
#             print ("After sublink")
#             #print (sublink)
#             sublink_data = requests.get(str(sublink))
#             sub_url_data = BeautifulSoup(sublink_data.text,"lxml")
#             time = sub_url_data.findAll('time')[0].text[:16]
#             time = time.strip()
#             print (time)
#             print ("After time")
#             time = dtt.strptime(str(time), "%B %d, %Y")
#             headline = sub_url_data.title.text
#             content = sub_url_data.findAll("p")
#             for i in range(3, len(content)):
#                 summary += content[i].text + " "
            
#             dictt.update({"Website":"Tomshardware.com","Category": "SSD", "url": sublink, "headline": str(headline), "summary": str(summary), "published_date": str(time)})
#             articles.append(dictt)
#         link += 1 
#         if (dtt.now()-time).days > 300:
#             booln = False

# For thessdreview.com

counter = 1
booln = True
while booln:    
    url = 'http://www.thessdreview.com/daily-news/latest-buzz/'
    url += "page/"+str(counter)+'/'
    print (url)
    print ("After url")
    counter += 1
    url_data = requests.get(url)
    url_data = BeautifulSoup(url_data.text,"lxml")
    data = url_data.findAll('article', {'class': 'item-list'})

    for item in data:
        dictt = {}
        links = item.findAll('a')
        time = item.findAll('span', {'class': 'tie-date'})[0].text
        time = dtt.strptime(str(time), "%B %d, %Y")
        
        for link in links:
            url = link['href']
            headline = link.text
            break
        summary = item.findAll('div',{'class': 'entry'})[0].text
        
        dictt.update({"Website":"thessdreview.com","Category": "SSD", "url": url, "headline": str(headline), "summary": str(summary), "published_date": str(time)})
        
        articles.append(dictt)
        if (dtt.now()-time).days > 300:
            booln = False

def get_only_text(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "lxml")
    text = ' '.join(map(lambda p: p.text, soup.find_all('p')))
    return(text)



driver = webdriver.PhantomJS('C:\\Users\\thulasiram.k\\Downloads\\phantomjs-2.1.1-windows\\phantomjs-2.1.1-windows\\bin\\phantomjs')
url = "https://news.samsung.com/global/"
driver.get(url)
html = driver.page_source.encode('utf-8')
page_num = 1
while True:

    try:
        driver.find_element_by_link_text(str(page_num)).click()
    except:
        pass
    page_num += 1
    if page_num > 56:
        break
html = driver.page_source.encode('utf-8')
data = BeautifulSoup(html, 'lxml')
headlines = data.findAll("div",{'class':'desc'})
for head in headlines:
    dictt = {}
    headline = head.text
    date = head.findAll('span', {'class': 'date'})
    date =  dtt.strptime(date[0].text,  "%B %d, %Y")
    url = head.findAll('a')[0]['href']
    text_data = get_only_text(url)
    final_summary = summarize(text_data)
    dictt.update({"Website":"news.samsung.com","Category": "SSD", "url": url, "headline": str(headline), "summary": str(text_data), "published_date": str(date)})
    articles.append(dictt)
write_into_csv(articles)       
