
import datetime
import requests
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import re
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
    file_name = "Headlines_final_3"+".csv" 
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
for r in data:
    dictt = {}
    url = r['href']
    headline_obj = r.findAll("h2", {"class": "headline"})
    summary_obj = r.findAll("p", {"class": "summary"})
    headline = headline_obj[0].contents[0]
    summary = summary_obj[0].contents[0]
    output = re.search(pattern,url)
    date = str(dtt.strptime(output.group(0), "%Y/%m/%d"))
    dictt.update({"Website":"Newyorktimes","Category": "Disruptive", "url": url, "headline": headline, "summary": str(summary), "published_date": date})
    #articles.append(dictt)

# For diplomat
#base_url = "https://thediplomat.com/category/blogs/page/"
# i = 0
# for i in range(2,120):
#     base_url = "https://thediplomat.com/category/blogs/page/"
#     base_url = base_url+str(i)
#     print (base_url)
#     diplomatnews_data = requests.get(base_url)
#     url_data2 = BeautifulSoup(diplomatnews_data.text,"lxml")
#     data2 = url_data2.findAll("div", {"class": "row"})
#     for r2 in data2:
#         dictt2 = {}
#         if i == 0:
#             i += 1
#             break
#         jj = r2.findAll("a")
#         url2 = jj[0]['href']
#         headline = r2.findAll("div", {"class": "postPrevTitle"})
#         if len(headline)>0:
#             if len(headline[0].contents)>0:
#                 headline2 = headline[0].contents[0]
#                 summary = r2.findAll("div", {"class": "postPrevTeaser prose"})
#                 if len(summary)>0:
#                     if len(summary[0].contents)>0:
#                         summary2 = summary[0].contents[0]
#                         date = r2.findAll("div", {"class": "postPrevSubtitle"})
#                         date2 = str(dtt.strptime(str(date[0].contents[0]), "%B %d, %Y"))
#                         #ab = date[0].contents[0]
#                         dictt2.update({"Website": "The Diplomat","Category": "GeoPolitics","url": url2, "headline": str(headline2), "summary": str(summary2), "published_date": date2})
#                         articles.append(dictt2)

#For CNN health news
# cnn_url = "https://edition.cnn.com/health"
# cnn_data = requests.get(cnn_url)

# url_data3 = BeautifulSoup(cnn_data.text,"lxml")
# data3 = url_data3.findAll("h3", {"class": "cd__headline"})
# pattern3 = r"(\d+/\d+/\d+)"
# for record in data3:
    
#     j = record.findAll("a")
#     sub_url = j[0]["href"]
#     if '/health/' in sub_url:
#         sub_url = "https://edition.cnn.com"+sub_url
#         output3 = re.search(pattern3,sub_url)
#         date3 = dtt.strptime(output3.group(0), "%Y/%m/%d")
#         date_now = dtt.now()
#         if (date_now - date3).days <60:
#             #print (sub_url)
#             #print ("After sub url")
#             sub_data = requests.get(sub_url)
#             url_data3 = BeautifulSoup(sub_data.text,"lxml")
#             title = url_data3.title.contents[0]
#             data3 = url_data3.findAll("li", {"class": "el__storyhighlights__item el__storyhighlights--normal"})
           
#             summary3 = ""
#             for ii in data3:
#                 summary3 += ii.contents[0]
#                 summary3 += " "
#             if len(summary3) >0:
#                 dictt3 = {}
                
#                 dictt3.update({"Website": "The CNN Health","Category": "Disruptive","url": sub_url, "headline": str(title), "summary": str(summary3), "published_date": str(date3)})
#                 #articles.append(dictt3)    

# For Guardian 

# wordnet_lemmatizer = WordNetLemmatizer()
# sid = SentimentIntensityAnalyzer()
# def get_only_text(url):
#     print (url)
#     print ("After url")
#     page = requests.get(url)
#     soup = BeautifulSoup(page.text, "lxml")
#     text = ' '.join(map(lambda p: p.text, soup.find_all('p')))
#     return(text)
# counter = 1

# boolen = True
# while boolen:
#     url_guardian = "https://www.theguardian.com/world/natural-disasters?page="
#     url_guardian += str(counter)
#     counter += 1
#     guardian_text = requests.get(url_guardian)
#     url_data4 = BeautifulSoup(guardian_text.text,"lxml")
#     objects = url_data4.findAll("div",{'class':'fc-container__inner'})
#     for objct in objects:
        
#         headline_date = objct.find("time")
#         date_obj = dtt.strptime(headline_date.text, "%d %B %Y")
#         if (dtt.now() - date_obj).days < 6:
#             for lk,hd in zip(objct.findAll("a",{'class':'fc-item__link'}), objct.findAll("span",{'class':'u-faux-block-link__cta fc-item__headline'})):
#                 link = lk['href']
#                 headline = hd.text
#                 text_data = get_only_text(lk['href'])
#                 words = sent_tokenize(text_data)
#                 words = [word.lower() for word in words]
#                 words = [wordnet_lemmatizer.lemmatize(word) for word in words]
#                 tf = TfidfVectorizer(analyzer='word', ngram_range=(1,3),
#                                       stop_words = 'english')
#                 tfidf_matrix =  tf.fit_transform(words)
#                 idf = tf.idf_
                
#                 dict_idf = dict(zip(tf.get_feature_names(), idf))
#                 sentences = sent_tokenize(text_data)
#                 sent_list = set()
#                 for sentence in sentences:
#                     for value, term in dict_idf.items():
#                         if value in sentence:
#                             sent_list.add(sentence)
#                 summary = summarize("\n".join(sent_list),ratio = 0.1)
#                 if len(summary) > 0:
#                     scores = sid.polarity_scores("\n".join(sent_list))
#                     dictt = {}
#                     dictt.update({"Website":"The Guardian Natural Distasters","Category": "Weather", "url": link, "headline": headline, "summary": str(summary), "published_date": str(date_obj)})
#                     #articles.append(dictt)
#         else:
#             boolen = False 


# counter = 1
# booln = True                    
# while booln:
        
#     url = 'https://geopolitics.co/page/'
#     url += str(counter)+'/'
#     counter += 1
#     print (url)
#     print ("After url")
#     url_data = requests.get(url)
#     url_data = BeautifulSoup(url_data.text,"lxml")
#     links = url_data.findAll("h1", {"class": "entry-title"})
#     for i in links:
#         dictt = {}
#         sublink = i.findAll('a')[0]['href']
#         #print (sublink)
#         sublink_data = requests.get(str(sublink))
#         sub_url_data = BeautifulSoup(sublink_data.text,"lxml")
#         time = sub_url_data.findAll('span', {'class': 'entry-date'})
#         date = dtt.strptime(time[0].time.text, "%B %d, %Y")
#         content = sub_url_data.findAll("p")
#         content = content[0].text
#         title = sub_url_data.title.text
#         title = title.strip(" | Covert Geopolitics")
#         #print (content[0].text)
#         #print ("after content")
#         dictt.update({"Website":"The geopolitics.co","Category": "Geopolitical", "url": sublink, "headline": str(title), "summary": str(content), "published_date": str(date)})
#         articles.append(dictt)
#     if (dtt.now()-date).days > 300:
#         booln = False
       

base_url_alerts = "http://geopoliticsalert.com/"
base_url_data_alerts = requests.get(base_url_alerts)
base_url_data_alerts = BeautifulSoup(base_url_data_alerts.text,"lxml")
links = base_url_data_alerts.findAll("h4", {"class": "news-title"})
for link in links:
    print (link)
    print ("After link")
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
    final_summary = summarize(summary)
    dictt.update({"Website": "Geopoliticsalert.com","Category": "Geopolitics","url": str(href), "headline": str(title), "summary": summary, "published_date": str(final_time)})
    articles.append(dictt)
   


write_into_csv(articles)