import gensim
import nltk
import time
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from gensim import corpora
from pymongo import MongoClient
import datetime

en_stop = set(nltk.corpus.stopwords.words('english'))
client = MongoClient('mongodb://localhost:27017')
db = client.Bachelor


def get_data(pipeline):
    # Expects the pipeline to be a pipelines used to aggregate a query in mongodb.
    # The resulting rows must contain '_id', 'vader', 'tb'
    # Where '_id' = labeling of said data, 'vader' = vader sentiment of the data
    # 'tb' = textblob sentiment of the data.
    print("---   Querying database   ---")
    start_time = time.time()
    data = db.Konrad.aggregate(pipeline, allowDiskUse=True)
    print("--- Done in: %.2f seconds ---" % (time.time() - start_time))
    return data


def get_lemma(word):
    return WordNetLemmatizer().lemmatize(word)


def process_text(line):
    tokens = word_tokenize(line)
    tokens = [token for token in tokens if token.isalnum()]
    tokens = [token for token in tokens if token not in en_stop]
    tokens = [get_lemma(token) for token in tokens]
    return tokens


def topic_dection(num_of_topics, num_of_words, passes, data):
    dictionary = corpora.Dictionary(data)
    term_matrix = [dictionary.doc2bow(doc) for doc in data]
    lda = gensim.models.ldamodel.LdaModel
    lda_model = lda(term_matrix, num_topics=num_of_topics, id2word=dictionary, passes=passes)
    with open('log.txt', 'a') as file:
        file.write(datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S:\n'))
        file.write('--- Result using parameters: ---\n')
        file.write(f"Number of topics: {num_of_topics}\n")
        file.write(f"Number of words {num_of_words}\n")
        file.write(f"Number of passes{passes}\n")
        file.write(str(lda_model.print_topics(num_topics=num_of_topics, num_words=num_of_words)) + "\n")
        file.write("--------------------------------\n")


def get_all_vaccine_articles():
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'uniqueId': 1,
                'year': {
                    '$year': '$date'
                },
                'normalized_title': 1,
                'normalized_description': 1,
                'is_vaccine': 1
            }
        }, {
            '$match': {
                'year': {
                    '$gte': 2020
                },
                'is_vaccine': True
            }
        }, {
            '$group': {
                '_id': '$uniqueId',
                'title': {
                    '$first': '$normalized_title'
                },
                'description': {
                    '$first': '$normalized_description'
                }
            }
        }, {
            '$project': {
                'text': {
                    '$concatArrays': [
                        '$title', '$description'
                    ]
                }
            }
        }
    ]
    lines_ = []
    for i in get_data(pipeline):
        lines_.append(i['text'])
    return lines_


if __name__ == '__main__':
    print("Lets fuck shit up!")
    lines = get_all_vaccine_articles()
    topic_dection(1, 10, 10, lines)
