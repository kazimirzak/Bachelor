import gensim
import csv
from gensim import corpora

if __name__ == '__main__':
    print("Reading file...........", end="")
    f = csv.reader(open('health.csv', encoding='utf-8'))
    l = [text[1] + text[2] for text in f]
    l = [x.split() for x in l]
    print("Done!\nCreating Dictionary....", end="")
    dictionary = corpora.Dictionary(l)
    print("Done!\nCreating term matrix...", end="")
    term_matrix = [dictionary.doc2bow(doc) for doc in l]
    print("Done!\nCreating LDA model.....", end="")
    lda = gensim.models.ldamodel.LdaModel
    print("Done!\nRunning LDA model......", end="")
    ldamodel = lda(term_matrix, num_topics=3, id2word=dictionary, passes=10)
    print("Done!")
    print(ldamodel.print_topics(num_topics=3, num_words=3))

