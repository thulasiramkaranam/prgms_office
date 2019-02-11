import datetime
import requests
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import re
from openpyxl import Workbook
from bs4 import BeautifulSoup
from datetime import datetime as dtt
#from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
#from gensim.summarization.summarizer import summarize
#from gensim.summarization import keywords
url = 'http://www.thessdreview.com/daily-news/latest-buzz/'
ab = requests.get(url)
url_data = BeautifulSoup(ab.text,"lxml")
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
    print (dictt)
    break

