import mysql.connector
import os
import fnmatch
import re
from dateutil import parser
import gzip
import json
import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

DATA_PATH = "E:\\release"

db = mysql.connector.connect(
    user="root",
    password="",
    database="bachelor"
)
cursor = db.cursor()

vader = SentimentIntensityAnalyzer()


# Inserts articles into the database using the DATA_PATH. Structure of data should be as described here: http://sciride.org/news.html#datacontent
def insert_articles():
    insert_query = "INSERT INTO articles (outlet, date, title, description, link) VALUES (%s, %s, %s, %s, %s)"
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
                        articles.append((outlet, date, title, description, link))
                try:
                    cursor.executemany(insert_query, articles)
                    db.commit()
                except:
                    print(outlet, date, title, description, link, sep='\n')


def sentiment_analysis():
    insert_vader = "INSERT INTO vader (articleId, neg, neu, pos, compound) VALUES (%s, %s, %s, %s, %s)"

    insert_textblob = "INSERT INTO textblob (articleId, polarity, subjectivity)" \
                      "VALUES (%s, %s, %s)"
    limit = 5000
    cursor.execute("SELECT MAX(id) FROM articles")
    num_of_articles = (cursor.fetchone())[0]
    cursor.execute("SELECT MAX(id) FROM vader")
    start_offset = (cursor.fetchone())[0]
    if not start_offset:
        start_offset = 0
    for i in range(start_offset, num_of_articles, limit):
        vs = []
        ts = []
        cursor.execute(f"SELECT id, title, description FROM articles WHERE id > {i} ORDER BY id ASC LIMIT {limit}")
        articles = cursor.fetchall()
        for article in articles:
            id = article[0]
            title = str(article[1])
            description = str(article[2])
            vs.append(vader_sentiment(id, f"{title} {description}"))
            ts.append(textblob_sentiment(id, f"{title} {description}"))
        cursor.executemany(insert_vader, vs)
        db.commit()
        cursor.executemany(insert_textblob, ts)
        db.commit()
        print(f"Processed Chunk: {i / limit}/{num_of_articles / limit}!")


def vader_sentiment(id, text):
    sentiment = vader.polarity_scores(text)
    # return a tuple with:
    # (articleId, neg, neu, pos, compound).
    return (id, sentiment['neg'], sentiment['neu'], sentiment['pos'], sentiment['compound'])


def textblob_sentiment(id, text):
    sentiment = TextBlob(text).sentiment
    # Returns a tuple with:
    # (articleId, polarity, subjectivity)
    return (id, sentiment.polarity, sentiment.subjectivity)


if __name__ == '__main__':
    print("Lets fuck shit up!")
    start_time = time.time()
    #insert_articles()
    sentiment_analysis()
    print("--- %s seconds ---" % (time.time() - start_time))



