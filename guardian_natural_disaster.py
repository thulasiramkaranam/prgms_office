
import requests
from bs4 import BeautifulSoup
from datetime import datetime as dtt
from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
from gensim.summarization.summarizer import summarize
from gensim.summarization import keywords

wordnet_lemmatizer = WordNetLemmatizer()
sid = SentimentIntensityAnalyzer()
def get_only_text(url):
    print (url)
    print ("After url")
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "lxml")
    text = ' '.join(map(lambda p: p.text, soup.find_all('p')))
    return(text)
counter = 1
articles = []
boolen = True
while boolen:
    url_guardian = "https://www.theguardian.com/world/natural-disasters?page="
    url_guardian += str(counter)
    counter += 1
    guardian_text = requests.get(url_guardian)
    url_data4 = BeautifulSoup(guardian_text.text,"lxml")
    objects = url_data4.findAll("div",{'class':'fc-container__inner'})
    for objct in objects:
        
        headline_date = objct.find("time")
        date_obj = dtt.strptime(headline_date.text, "%d %B %Y")
        if (dtt.now() - date_obj).days < 60:
            for lk,hd in zip(objct.findAll("a",{'class':'fc-item__link'}), objct.findAll("span",{'class':'u-faux-block-link__cta fc-item__headline'})):
                link = lk['href']
                headline = hd.text
                text_data = get_only_text(lk['href'])
                words = sent_tokenize(text_data)
                words = [word.lower() for word in words]
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
                summary = summarize("\n".join(sent_list),ratio = 0.1)
                scores = sid.polarity_scores("\n".join(sent_list))
                dictt = {}
                dictt.update({"Website":"The Guardian Natural Distasters","Category": "Weather", "url": link, "headline": headline, "summary": str(summary), "published_date": str(date_obj)})
                articles.append(dictt)
        else:
            boolen = False
f = open ("guardian_list.txt", "w", encoding = "utf-8")
f.write(str(articles))
f.close()

