from pprint import pprint
import codecs
import spacy
import csv
from spacy import displacy
from collections import Counter
nlp = spacy.load('en_core_web_sm')

def read_from_csv_file(path):
    csv_file = csv.reader(open(path, encoding='utf-8'))
    lines = [f"{text[1]} {text[2]}" for text in csv_file if len(text) >= 1]
    return lines


if __name__ == '__main__':
    print("Lets fuck shit up!")
    lines = read_from_csv_file('health.csv')
    labels = []
    ents = []
    for num, line in enumerate(lines):
        result = nlp(line)
        labels.extend([x.label_ for x in result.ents])
        ents.extend([(x.text, x.label_) for x in result.ents])
        if num % 1000 == 0:
            print(f"Progress: {int((num / len(lines)) * 100)}%")
    f = codecs.open('labels.csv', 'w', encoding='utf8')
    f.write("label, count\n")
    for (label, count) in Counter(labels).most_common():
        f.write(f"{label}, {count}\n")
    f.close()
    f = codecs.open('entities.csv', 'w', encoding='utf8')
    f.write("entity, label, count\n")
    for ((entity, label), count) in Counter(ents).most_common():
        f.write(f"{entity}, {label}, {count}\n")
    f.close()
    #print(Counter(labels).most_common())
    #print(Counter(ents).most_common())
