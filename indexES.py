from sys import path
import nltk
# nltk.download()
from nltk.corpus import stopwords
import sys
import json
import os
import re
from elasticsearch import Elasticsearch, helpers
import requests
import traceback
from tika import parser
from pathlib import Path

res = requests.get("http://localhost:9200")

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

folderpath = r".\all_files" 
filepaths  = [os.path.join(folderpath, name) for name in os.listdir(folderpath)]

i=1
j=1
k=1
m=1
s=1
content = ''
for path in filepaths:
    # print("processing file " + path)
    try:
        parsed = parser.from_file(path)
        obj = parsed["content"]
        sentences = nltk.sent_tokenize(obj)
        nouns = []
        paths = []
        error_path = []
    
        for sentence in sentences:
            for word,pos in nltk.pos_tag(nltk.word_tokenize(str(sentence))):
                count = 0
                word = word.lower()
                if (pos == 'NN' or pos == 'NNP' or pos == 'NNS' or pos == 'NNPS'):
                    if word not in nouns and len(word) > 3:
                        nouns.append(word)
                    else :
                        count += 1
        paths.append(path)


    except Exception as err:
        # traceback.print_tb(err.__traceback__)
        print("error in file path " + path)
        error_path.append(path)
        continue
    
    # print(nouns, paths)
    # print(paths)
    
    lists  = ['nouns', 'paths']
    data = {keyword: globals()[keyword] for keyword in lists}
    with open('file'+ str(i) +'.json', 'w') as outfile:
        i += 1
        json.dump(data, outfile, indent=4)
    with open ('file'+ str(j) +'.json', 'r') as f:
        content = f.read()
        j += 1
    # print(content)

    es.index(index='data15', doc_type='doc', id = k, body=json.loads(content))
    k += 1

    # os.remove('file'+ str(s) +'.json')
    # s += 1

    
    # print(content)
    print("Processing...")
# print(error_path)
print("Data Indexed !!!")





























    # time 9-10 sec