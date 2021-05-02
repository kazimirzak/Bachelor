import fnmatch
import gzip
import json
import os
import re
import sys
import csv
import time
import nltk
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from dateutil import parser
from pymongo import MongoClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
from math import floor
en_stop = set(nltk.corpus.stopwords.words('english'))

DATA_PATH = "E:\\bachelor\\release\\"
client = MongoClient('mongodb://localhost:27017')
db = client.Bachelor
vaccine_keywords = ['vaccine', 'vaccinate', 'vaccination', 'immunization']
vader = SentimentIntensityAnalyzer()


def insert_articles():
    with open('result.csv', newline='') as result_file:
        writer = csv.writer(result_file)
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
                        text = f'{title} {description}'
                        link = next(iter(article['link'])).strip()
                        is_covid = article['is_covid']
                        normalized_title = process_text(title)
                        normalized_description = process_text(description)
                        is_vaccine = determine_vaccine(normalized_title, normalized_description)
                        if title and description:
                            articles.append(
                                {
                                    "uniqueId": article_id,
                                    "outlet": outlet,
                                    "date": date,
                                    "title": title,
                                    "description": description,
                                    "normalized_title": normalized_title,
                                    "normalized_description": normalized_description,
                                    "is_covid": is_covid,
                                    "is_vaccine": is_vaccine,
                                    "vader": vader.polarity_scores(text)['compound'],
                                    "tb": TextBlob(text).sentiment.polarity,
                                    "link": link
                                }
                            )
                    try:
                        db.Konrad.insert_many(articles)
                    except:
                        print(sys.exc_info()[0])
                        print(article_id, outlet, date, title, description, normalized_title, normalized_description, is_covid, is_vaccine, link, sep='\n')


def determine_vaccine(title, description):
    for keyword in vaccine_keywords:
        for word in title:
            if keyword.lower() == word.lower():
                return True
        for word in description:
            if keyword.lower() == word.lower():
                return True
    return False


def process_text(line):
    tokens = word_tokenize(line)
    tokens = [token.lower() for token in tokens if token.isalnum()]
    tokens = [token for token in tokens if token not in en_stop]
    tokens = [get_lemma(token) for token in tokens]
    return tokens


def get_lemma(word):
    return WordNetLemmatizer().lemmatize(word)


def sentiment_analysis():
    articles = db.Konrad.count()
    print("Number of articles: ", articles)
    i = 0
    for item in db.Konrad.find({}, {"uniqueId": 0, "outlet": 0, "date": 0, "link": 0}):
        text = f"{item['title']} {item['description']}"
        db.Konrad.update_one({"_id": item["_id"]}, {
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
    #sentiment_analysis()
    print("--- %s seconds ---" % (time.time() - start_time))


