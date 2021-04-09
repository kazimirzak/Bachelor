import fnmatch
import gzip
import json
import os
import re
import sys
import time

from dateutil import parser
from pymongo import MongoClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
from math import floor

DATA_PATH = "E:\\articles"
client = MongoClient('mongodb://localhost:27017')
db = client.Bachelor

vader = SentimentIntensityAnalyzer()


def insert_articles():
    for path, dirs, files in os.walk(DATA_PATH):
        for f in fnmatch.filter(files, '*.gz'):
            abs_path = os.path.abspath(os.path.join(path, f))
            match = re.search(r'\\([^\\]*)\\per_day\\(\d*)\.gz$', abs_path)
            outlet = match.group(1).strip()
            date = parser.parse(match.group(2))
            with gzip.open(abs_path) as zip:
                content = json.loads(zip.read())
                articles = []
                for article_id, article in content.items():
                    title = article['title'].strip()
                    description = article['description'].strip()
                    link = next(iter(article['link'])).strip()
                    if title and description:
                        articles.append(
                            {
                                "uniqueId": article_id,
                                "outlet": outlet,
                                "date": date,
                                "title": title,
                                "description": description,
                                "link": link
                            }
                        )
                try:
                    db.Articles.insert_many(articles)
                except:
                    print(sys.exc_info()[0])
                    #print(outlet, date, title, description, link, sep='\n')


def sentiment_analysis():
    articles = db.Articles.count()
    print("Number of articles: ", articles)
    i = 0
    for item in db.Articles.find({}, {"uniqueId": 0, "outlet": 0, "date": 0, "link": 0}):
        text = f"{item['title']} {item['description']}"
        db.Articles.update_one({"_id": item["_id"]}, {
           "$set": {
               "vader": vader.polarity_scores(text)['compound'],
               "tb": TextBlob(text).sentiment.polarity

           }
        })
        i = i + 1
        print(f"\rProcessing... {floor(i / articles * 100)}%", end="")
    print()


if __name__ == '__main__':
    print("Lets fuck shit up!")
    start_time = time.time()
    insert_articles()
    sentiment_analysis()
    print("--- %s seconds ---" % (time.time() - start_time))


