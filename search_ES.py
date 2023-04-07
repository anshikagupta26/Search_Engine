from sys import path
import nltk
# nltk.download()
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import sys
import json
import os
import re
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import bulk
# from flask import Flask , render_template, url_for, flash, redirect
import requests
import traceback
from tika import parser
from pathlib import Path
# import indexES
import csv

def connect2ES():
    es = Elasticsearch([{"host": "localhost", "port": 9200}], timeout=60, max_retries=10, retry_on_timeout=True)
    if es.ping():
        print("Connect to ES!")
    else:
        print("Could not connect")
        sys.exit()
        
    print("************************************************************")
    return es

def keywordSearch(es, q):
    #processing text
    # print(q)
    q = re.sub(r"\[[0-9]*\]", " ",q)
    q = q.lower()
    q = re.sub(' +', ' ', q)
    q = re.sub(r'[^\w\s]', '', q)

    #remove stopwords
    text_tokens = word_tokenize(q)
    tokens_without_sw = [word for word in text_tokens if not word in stopwords.words()]
    q = (" ").join(tokens_without_sw)

    b = {
        "_source": [
            "paths"
        ],
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "nouns": q
                        }
                    }
                ]
            }
        }
    }


    # print(b)
    res = es.search(index="data15", doc_type='doc', body=b)
    return res

es = connect2ES()

def search(query):
    # print(query)
    q = query.replace("+", " ")
    res_kw = keywordSearch(es, q)
    
    for hit in res_kw["hits"]["hits"]:
        print(hit["_source"]["paths"])


if __name__ == "__main__":
    print("search here")
    word = input()
    search(word)













































# count.
# showing result in decreasing count of words

# filtering result acc to preferences