
# coding: utf-8

# In[1]:


from bs4 import BeautifulSoup
import requests
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer
from textblob import TextBlob
import nltk
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from gensim.summarization.summarizer import summarize
from gensim.summarization import keywords


# In[2]:


USER_AGENT = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}


# In[3]:


Bag = ["Flood",'Storm','Hurricane','heavy rain','Fire','drought','Earthquake']
Sources = ["https://globalnews.ca/weather/","https://www.theguardian.com/world/natural-disasters",
           "https://www.accuweather.com/en/weather-blogs/international",
           "https://www.reuters.com/subjects/natural-disasters"]


# In[4]:


#for url in Sources:
url_data = requests.get(Sources[0],headers=USER_AGENT)
html = BeautifulSoup(url_data.text,"lxml")
header_data = html.findAll("h3",{'class':'story-h'})  


# In[5]:


def get_only_text(url):
    page = requests.get(url,headers = USER_AGENT)
    soup = BeautifulSoup(page.text, "lxml")
    text = ' '.join(map(lambda p: p.text, soup.find_all('p')))
    return(text)


# In[ ]:


from selenium import webdriver
import time

 
url = "https://globalnews.ca/weather/"
driver = webdriver.PhantomJS("D:\\BUSINESS DATA\\Work\\phantomjs-2.1.1-windows\\bin\\phantomjs")
driver.get(url)
html = driver.page_source.encode('utf-8')
page_num = 0

while driver.find_element_by_name('2'):
    driver.find_element_by_name('2').click()
    page_num += 1
    if page_num > 250:
        break
    #print("getting page number "+str(page_num))

html = driver.page_source.encode('utf-8')


# In[ ]:


from datetime import datetime
wordnet_lemmatizer = WordNetLemmatizer()
sid = SentimentIntensityAnalyzer()
date_ = []
hline_ = []
lnk_ = []
Summary = []
Scor = []
data = BeautifulSoup(html)
headlines = data.findAll('div',{'class':'story story-float-img'})
for head in headlines:
    hline = head.find('h3')
    time = head.find('span')
    if "September" in time.text and "2018" in time.text:
        if any(x in hline.text.split(' ') for x in Bag):
            hline_.append(hline.text)
            date_.append(time.text) 
            link = head.findAll("h3",{'class':'story-h'})
            for lnk in link:
                lk = lnk.find('a',href = True)
                lnk_.append(lk['href'])
                text_data = get_only_text(lk['href'])
                words = sent_tokenize(text_data)
                freqTable = dict()
                words = [word.lower() for word in words]
                #words = [word for word in words if word.isalnum()]
                words = [wordnet_lemmatizer.lemmatize(word) for word in words]
                tf = TfidfVectorizer(analyzer='word', ngram_range=(1,3),
                                 stop_words = 'english')
                tfidf_matrix =  tf.fit_transform(words)
                idf = tf.idf_
                dict_idf = dict(zip(tf.get_feature_names(), idf))
                sentences = sent_tokenize(text_data)
                sent_list = set()
                for sentence in sentences:
                    for value, term in dict_idf.items():
                        if value in sentence:
                            sent_list.add(sentence)
                Summary.append(summarize("\n".join(sent_list),ratio = 0.1))
                scores = sid.polarity_scores("\n".join(sent_list))
                Scor.append(scores['compound'])


# In[ ]:


pd.DataFrame({"Date":date_, "Headline":hline_,"url":lnk_,"Summary":Summary,"Sentiments":Scor}).to_excel("globalnews.xlsx")


# In[ ]:


#for url in Sources:
url_data = requests.get(Sources[1],headers=USER_AGENT)
html = BeautifulSoup(url_data.text,"lxml")  


# In[ ]:


from selenium import webdriver
import time

 
url = "https://www.theguardian.com/world/natural-disasters"
driver = webdriver.PhantomJS("D:\\BUSINESS DATA\\Work\\phantomjs-2.1.1-windows\\bin\\phantomjs")
driver.get(url)
page_num = 1
date_ = []
hline_ = []
lnk_ = []
Summary = []
Scor = []
while True:
    driver.get(url + "?page=" + str(page_num))
    page_num += 1
    if page_num > 25:
        break
    html = driver.page_source.encode('utf-8')
    from datetime import datetime
    wordnet_lemmatizer = WordNetLemmatizer()
    sid = SentimentIntensityAnalyzer()
   
    data = BeautifulSoup(html)
    headlines = data.findAll("div",{'class':'fc-container__inner'})
    for head in headlines:
        date = head.find("time")
        if "September" in date.text:
            for lk,hd in zip(head.findAll("a",{'class':'fc-item__link'}), head.findAll("span",{'class':'u-faux-block-link__cta fc-item__headline'})):
                #if any(x in hd.text.split(' ') for x in Bag):
                lnk_.append(lk['href'])
                date_.append(date.text)
                hline_.append(hd.text)
                text_data = get_only_text(lk['href'])
                words = sent_tokenize(text_data)
                freqTable = dict()
                words = [word.lower() for word in words]
                #words = [word for word in words if word.isalnum()]
                words = [wordnet_lemmatizer.lemmatize(word) for word in words]
                tf = TfidfVectorizer(analyzer='word', ngram_range=(1,3),
                                  stop_words = 'english')
                tfidf_matrix =  tf.fit_transform(words)
                idf = tf.idf_
                dict_idf = dict(zip(tf.get_feature_names(), idf))
                sentences = sent_tokenize(text_data)
                sent_list = set()
                for sentence in sentences:
                    for value, term in dict_idf.items():
                        if value in sentence:
                            sent_list.add(sentence)
                Summary.append(summarize("\n".join(sent_list),ratio = 0.1))
                scores = sid.polarity_scores("\n".join(sent_list))
                Scor.append(scores['compound'])


# In[ ]:


pd.DataFrame({"Date":date_, "Headline":hline_,"url":lnk_,"Summary":Summary,"Sentiments":Scor}).to_excel("thegaurdian.xlsx")


# In[6]:


from selenium import webdriver
import time

 
url = "https://www.accuweather.com/en/weather-news"
driver = webdriver.PhantomJS("D:\\BUSINESS DATA\\Work\\phantomjs-2.1.1-windows\\bin\\phantomjs")
driver.get(url)
page_num = 1
date_ = []
hline_ = []
lnk_ = []
Summary = []
Scor = []
while True:
    driver.get(url + "?page=" + str(page_num))
    page_num += 1
    if page_num > 25:
        break
    html = driver.page_source.encode('utf-8')
    from datetime import datetime
    wordnet_lemmatizer = WordNetLemmatizer()
    sid = SentimentIntensityAnalyzer()
   
    data = BeautifulSoup(html)
    headlines = data.findAll("div",{'class':'info'})
    for head in headlines:
        date = head.find("h5")
        if date != None:
            linkk = head.find("a")
            if "September" in date.text:
                lnk_.append(linkk['href'])
                date_.append(date.text.strip())
                hline_.append(linkk.text)
                text_data = get_only_text(linkk['href'])
                words = sent_tokenize(text_data)
                freqTable = dict()
                words = [word.lower() for word in words]
                #words = [word for word in words if word.isalnum()]
                words = [wordnet_lemmatizer.lemmatize(word) for word in words]
                tf = TfidfVectorizer(analyzer='word', ngram_range=(1,3),
                                   stop_words = 'english')
                tfidf_matrix =  tf.fit_transform(words)
                idf = tf.idf_
                dict_idf = dict(zip(tf.get_feature_names(), idf))
                sentences = sent_tokenize(text_data)
                sent_list = set()
                for sentence in sentences:
                    for value, term in dict_idf.items():
                        if value in sentence:
                            sent_list.add(sentence)
                Summary.append(summarize("\n".join(sent_list),ratio = 0.1))
                scores = sid.polarity_scores("\n".join(sent_list))
                Scor.append(scores['compound'])


# In[7]:


pd.DataFrame({"Date":date_, "Headline":hline_,"url":lnk_,"Summary":Summary,"Sentiments":Scor}).to_excel("accuweather.xlsx")


# In[8]:


from selenium import webdriver
import time
import datetime
now = datetime.datetime.now()
 
url = "https://www.reuters.com/news/archive/tsunami?view=page&page=1&pageSize=10"
driver = webdriver.PhantomJS("D:\\BUSINESS DATA\\Work\\phantomjs-2.1.1-windows\\bin\\phantomjs")
driver.get(url)
page_num = 1
date_ = []
hline_ = []
lnk_ = []
Summary = []
Scor = []
while True:
    driver.get("https://www.reuters.com/news/archive/tsunami?view=page&page=" +str(page_num)+ "&pageSize=10")
    page_num += 1
    if page_num > 20:
        break
    html = driver.page_source.encode('utf-8')
    from datetime import datetime
    wordnet_lemmatizer = WordNetLemmatizer()
    sid = SentimentIntensityAnalyzer()
   
    data = BeautifulSoup(html)
    headlines = data.findAll("div",{'class':'story-content'})
    for head in headlines:
        hd = head.find("h3")
        lnkk = head.find("a")
        date = head.find("span")
        if date != None:
            if ("Sep" in date.text or "Oct" in date.text):
                date_.append(date.text.strip())
                hline_.append(hd.text.strip())
                lnk_.append(lnkk['href'])
                text_data = get_only_text("https://www.reuters.com/" + lnkk['href'])
                words = sent_tokenize(text_data)
                freqTable = dict()
                words = [word.lower() for word in words]
                #words = [word for word in words if word.isalnum()]
                words = [wordnet_lemmatizer.lemmatize(word) for word in words]
                tf = TfidfVectorizer(analyzer='word', ngram_range=(1,3),
                                       stop_words = 'english')
                tfidf_matrix =  tf.fit_transform(words)
                idf = tf.idf_
                dict_idf = dict(zip(tf.get_feature_names(), idf))
                sentences = sent_tokenize(text_data)
                sent_list = set()
                for sentence in sentences:
                    for value, term in dict_idf.items():
                        if value in sentence:
                            sent_list.add(sentence)
                Summary.append(summarize("\n".join(sent_list),ratio = 0.1))
                scores = sid.polarity_scores("\n".join(sent_list))
                Scor.append(scores['compound'])


# In[9]:


pd.DataFrame({"Date":date_, "Headline":hline_,"url":lnk_,"Summary":Summary,"Sentiments":Scor}).to_excel("reuters.xlsx")

