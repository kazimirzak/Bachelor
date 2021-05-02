import gensim
import nltk
import csv
import time
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from gensim import corpora
from pymongo import MongoClient

#nltk.download('wordnet')
#nltk.download('stopwords')
#nltk.download('punkt')
en_stop = set(nltk.corpus.stopwords.words('english'))
client = MongoClient('mongodb://localhost:27017')
db = client.Bachelor


def get_lemma(word):
    return WordNetLemmatizer().lemmatize(word)


def process_text(line):
    tokens = word_tokenize(line)
    tokens = [token for token in tokens if len(token) > 4]
    tokens = [token for token in tokens if token not in en_stop]
    tokens = [get_lemma(token) for token in tokens]
    return tokens


def read_from_csv_file(path):
    #print("Reading file...........", end="")
    csv_file = csv.reader(open(path, encoding='utf-8'))
    lines = [f"{text[1]} {text[2]}" for text in csv_file if len(text) >= 1]
    return lines


def read_from_mongodb():
    pipeline = [
        {
            "$group":
                {
                    "_id": "$uniqueId",
                    "title": {"$first": "$title"},
                    "description": {"$first": "$description"}
                }
        }
    ]
    return get_data(pipeline)


def get_data(pipeline):
    #print("---            Querying database           ---")
    start_time = time.time()
    data = db.Articles.aggregate(pipeline, allowDiskUse=True)
    documents = [f"{item['title']} {item['description']}" for item in data]
    #print(f"--- Fetched {len(documents)} items in: %.2f seconds ---" % (time.time() - start_time))
    return documents

def topic_detection(num_of_topics, num_of_words, passes, data):
    dictionary = corpora.Dictionary(data)
    term_matrix = [dictionary.doc2bow(doc) for doc in data]
    lda = gensim.models.ldamodel.LdaModel
    ldamodel = lda(term_matrix, num_topics=num_of_topics, id2word=dictionary, passes=passes)
    print("--- Result using parameters: ---")
    print("Number of topics:", num_of_topics)
    print("Number of words: ", num_of_words)
    print("Number of passes:", passes)
    print(ldamodel.print_topics(num_topics=num_of_topics, num_words=num_of_words))
    print("--------------------------------")


if __name__ == '__main__':
    print("Our Collected Data:")
    data = []
    lines = read_from_csv_file('health.csv')
    for line in lines:
        tokens = process_text(line)
        data.append(tokens)
    for passes in [10, 25, 100]:
        for num_of_words in [1, 3]:
            for num_of_topics in [1, 3, 5]:
                topic_detection(num_of_topics, num_of_words, passes, data)

    print("Konrads Data:")
    data = []
    lines = read_from_mongodb()
    for line in lines:
        tokens = process_text(line)
        data.append(tokens)
    for passes in [10, 25, 100]:
        for num_of_words in [1, 3]:
            for num_of_topics in [1, 3, 5]:
                topic_detection(num_of_topics, num_of_words, passes, data)
