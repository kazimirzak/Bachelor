from gensim import corpora
from gensim.models.ldamodel import LdaModel as lda
import datetime
from os import walk
import pandas as pd
import codecs


def topic_detection(num_of_topics, texts, file_name):
    dictionary = corpora.Dictionary(texts)
    term_matrix = [dictionary.doc2bow(doc) for doc in texts]
    lda_model = lda(term_matrix, num_topics=num_of_topics, id2word=dictionary)
    with codecs.open(file_name, 'w', encoding='utf-8') as file_:
        file_.write(datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S:\n'))
        file_.write('--- Result using parameters: ---\n')
        file_.write(f"Number of topics: {num_of_topics}\n")
        file_.write('[\n')
        for i in lda_model.print_topics(num_topics=num_of_topics, num_words=20):
            file_.write(str(i) + '\n')
        file_.write(']\n')
        file_.write("--------------------------------\n")


if __name__ == '__main__':
    print("Lets fuck shit up!")
    DIR = './data/'
    ignored_files = ['CoviVac.csv', 'EpiVacCorona.csv']
    num_of_topics = {
        'all_vaccine.csv': 4,
        'AstraZeneca.csv': 4,
        'Biontech.csv': 4,
        'EU.csv': 4,
        'GOP.csv': 4,
        'Johnson & Johnson.csv': 4,
        'Moderna.csv': 4,
        'Pfizer.csv': 4,
        'Sputnik V.csv': 4
    }
    _, _, filenames = next(walk(DIR))
    for file in [file for file in filenames if file.endswith('.csv')]:
        if file in ignored_files:
            continue
        print(file)
        brand = file.replace('.csv', '')
        texts = []
        for index, data in pd.read_csv(DIR + file, index_col=[0]).iterrows():
            texts.append(data['text'].replace("'", "").strip('"[]').split(', '))
        topic_detection(num_of_topics[file], texts, f'./results/{brand}.txt')

