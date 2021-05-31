import re
from collections import Counter
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set(rc={'font.size': 10, 'axes.titlesize': 16, 'axes.labelsize': 10, 'figure.figsize': (11.7, 8.27)})
sns.set(font_scale=1.25)
sns.set_style('ticks')

if __name__ == '__main__':
    print("Lets fuck shit up!")
    mapping = dict()
    with open('all_vaccine.txt', 'r') as file:
        num_of_topics = None
        for line in file.readlines():
            if line.startswith('Number of topics: '):
                num_of_topics = line.split(' ')[-1]
                mapping[num_of_topics] = []
                continue
            if num_of_topics is None:
                continue
            mapping[num_of_topics].extend(re.findall(r'"([a-z]+)"', line))
    for key in mapping:
        mapping[key] = Counter(mapping[key]).most_common(10)
    num_of_topics = []
    words = []
    counts = []
    for num_of_topic, occurences in mapping.items():
        for (word, count) in occurences:
            num_of_topics.append(num_of_topic)
            words.append(word)
            counts.append(count)
    xlabel = 'Number of topics'
    ylabel = 'Frequency'
    zlabel = 'Word'
    df = pd.DataFrame(data={xlabel: num_of_topics, ylabel: counts, zlabel: words})
    ax = sns.factorplot(x=xlabel, y=ylabel, hue=zlabel, data=df, kind='bar')
    plt.title('Frequency of words grouped by the number of topics')
    plt.show()

