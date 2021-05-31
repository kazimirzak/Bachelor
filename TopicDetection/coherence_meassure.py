from gensim import corpora
from gensim.models.ldamodel import LdaModel
from gensim.models.coherencemodel import CoherenceModel
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(rc={'font.size': 10, 'axes.titlesize': 16, 'axes.labelsize': 10, 'figure.figsize': (11.7, 8.27)})
sns.set(font_scale=1.25)
sns.set_style('ticks')


def compute_coherence_values(dictionary, term_matrix, texts, limit, start, step):
    coherence_values = []
    model_list = []
    print("Start:", start)
    print("Step:", step)
    print("Limit:", limit)
    for num_topics in range(start, limit, step):
        print(num_topics)
        model = LdaModel(corpus=term_matrix, id2word=dictionary, num_topics=num_topics)
        model_list.append(model)
        coherence_model = CoherenceModel(model=model, texts=texts, dictionary=dictionary, coherence='c_v')
        coherence_values.append(coherence_model.get_coherence())
    return model_list, coherence_values


if __name__ == '__main__':
    print('Lets fuck shit up!')
    start = 1
    step = 1
    limit = 40
    texts = []
    file = 'data/Sputnik V.csv'
    print(file)
    for index, data in pd.read_csv(file, index_col=[0]).iterrows():
        texts.append(data['text'].replace("'", "").strip('"[]').split(', '))
    dictionary = corpora.Dictionary(texts)
    term_matrix = [dictionary.doc2bow(doc) for doc in texts]
    model_list, coherence_values = compute_coherence_values(
        dictionary=dictionary,
        term_matrix=term_matrix,
        texts=texts,
        limit=limit,
        start=start,
        step=step
    )
    x = range(start, limit, step)
    xlabel = 'Number of topics'
    ylabel = 'Coherence score'
    df = pd.DataFrame(data={xlabel: x, ylabel: coherence_values})
    fig, axes = plt.subplots()
    ax = sns.lineplot(x=xlabel, y=ylabel, data=df, ax=axes)
    ax.set_title('Coherence score compared to number of topics detected')
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    plt.show()
