import codecs
from collections import Counter

import nltk
import csv
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import word_tokenize
en_stop = set(nltk.corpus.stopwords.words('english'))

def get_lemma(word):
    return WordNetLemmatizer().lemmatize(word)


def process_text(line):
    tokens = word_tokenize(line)
    tokens = [token for token in tokens if token.isalnum()]
    tokens = [token for token in tokens if token not in en_stop]
    tokens = [get_lemma(token) for token in tokens]
    return tokens


def read_from_csv_file(path):
    csv_file = csv.reader(open(path, encoding='utf-8'))
    lines = [f"{text[1]} {text[2]}" for text in csv_file if len(text) >= 1]
    return lines


if __name__ == '__main__':
    print("Lets fuck shit up")
    lines = read_from_csv_file('health.csv')
    occurences = Counter()
    for num, line in enumerate(lines):
        tokens = process_text(line)
        occurences.update(tokens)
        if num % 1000 == 0:
            print(f"Progress: {int((num / len(lines)) * 100)}%")
    f = codecs.open('occurences.csv', 'w', encoding='utf8')
    f.write("word, count\n")
    for word, count in occurences.most_common():
        f.write(f"{word}, {count}\n")
    f.close()
