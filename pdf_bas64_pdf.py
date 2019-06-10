


import base64
import PyPDF2
import io
u = open(r'C:\Users\thulasiram.k\Downloads\doc.pdf', 'rb')
# encoding a file
es = base64.b64encode(u.read())
#sending the es like below to api
"""
{
    "data": es
}
"""

#decoding a file
#reading in the api

decoded_obj = base64.b64decode(es)
toread = io.BytesIO()
toread.write(decoded_obj)
toread.seek(0)
pf = PyPDF2.PdfFileReader(toread)
pfp = pf.getPage(0)
#you will get the data in api

print(pfp.extractText())
