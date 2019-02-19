



from bs4 import BeautifulSoup
import requests

bag_of_words = ['topic', 'topics', 'tag', 'tags']
not_required_words = ['instagram', 'twitter']
booln = True
links = []
article_tags = [] 
def tag_from_div(soup):
    class_string = "keywords"
    soup_data = soup.findAll("div", {"class": class_string})
    #tags = soup_data.findAll("a")
    print(len(soup_data))
    print("After length of data")
    if len(soup_data) > 0:
        tags = soup_data[0].findAll("a")
        for tag in tags:
            url = tag.get('href')
            contents = tag.contents
            if len(contents) > 0:
                article_tags.append(contents[0].strip())
    return article_tags
          

def tags_from_url(soup):
    
    for link in soup.findAll('a'):
        
        if any(x in str(link) for x in bag_of_words) and all(x not in str(link) for x in not_required_words):
            
            print(link)
            print("After link")
            links.append(link)
    
    for l in links:
        
        clas = l.get('class')
        
        if clas is not None and any(t in clas[0] for t in bag_of_words):
            
            booln = False
            url = l.get('href')
            content = l.contents
            if len(content) > 0:
                article_tags.append(content[0].strip())
            print("After content")
    if booln is True:
        for nl in links:
            nurl = nl.get('href')
            ncontent = nl.contents
            if len(ncontent) > 0:
                article_tags.append(ncontent[0].strip())
    return article_tags
            

