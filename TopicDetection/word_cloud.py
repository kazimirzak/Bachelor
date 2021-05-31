import math
import re
from os import walk

import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud

plt.rcParams.update({
    "text.usetex": True,
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica"]})
sns.set(rc={'font.size': 10, 'axes.titlesize': 16, 'axes.labelsize': 10, 'figure.figsize': (11.7, 8.27)})
sns.set(font_scale=1.25)
sns.set_style('ticks')


def plot_2d(topics, num_rows, num_columns, title, filename):
    fig, axis = create_subplots(num_rows, num_columns, title)
    for i, topic in enumerate(topics):
        row = i // num_columns
        col = i % num_columns
        create_axis(topic, axis[row, col])
    save_image(filename)
    #plt.show()


def plot_1d(topics, num_rows, num_columns, title, filename):
    fig, axis = create_subplots(num_rows, num_columns, title)
    for i, topic in enumerate(topics):
        create_axis(topic, axis[i])
    save_image(filename)
    plt.show()


def create_subplots(num_rows, num_columns, title):
    fig, axis = plt.subplots(num_columns, num_rows)
    fig.subplots_adjust(hspace=-0.68, wspace=0, top=1.23)
    fig.size = (11.7, 8.27)
    fig.suptitle(title)
    return fig, axis


def create_axis(topic, axis):
    wc = WordCloud(background_color='white', random_state=42, colormap='Dark2')
    wc = wc.generate_from_frequencies(topic)
    axis.imshow(wc, interpolation='bilinear')
    axis.get_xaxis().set_ticks([])
    axis.get_yaxis().set_ticks([])


def save_image(filename):
    plt.savefig(filename, bbox_inches='tight')


if __name__ == '__main__':
    print('Lets fuck shit up!')
    options = {
        'all_vaccine.txt': {
            'title': 'All vaccine articles',
            'filename': 'all_vaccine.png'
        },
        'AstraZeneca.txt': {
            'title': 'AstraZeneca',
            'filename': 'AstraZeneca.png'
        },
        'Biontech.txt': {
            'title': 'BioNTech',
            'filename': 'BioNTech.png'
        },
        'EU.txt': {
            'title': 'European Union (EU)',
            'filename': 'EU.png'
        },
        'GOP.txt': {
            'title': 'The Republican Party (GOP)',
            'filename': 'GOP.png'
        },
        'Johnson & Johnson.txt': {
            'title': 'Johnson \& Johnson',
            'filename': 'JohnsonJohnson.png'
        },
        'Moderna.txt': {
            'title': 'Moderna',
            'filename': 'Moderna.png'
        },
        'Pfizer.txt': {
            'title': 'Pfizer',
            'filename': 'Pfizer.png'
        },
        'Sputnik V.txt': {
            'title': 'Sputnik V',
            'filename': 'sputnikV.png'
        }
    }
    DIR = './results/'
    PIC_DIR = 'E:\\bachelor\\dataIsButyful\\OfficialPics\\Results\\TopicDetection\\'
    files = dict()
    _, _, filenames = next(walk(DIR))
    for filename in [file for file in filenames if file.endswith('.txt')]:
        topics = []
        with open(DIR + filename, 'r') as file:
            for line in file.readlines():
                matches = re.findall(r'(\d\.\d\d\d)\*\"([a-z]+)\"', line)
                if len(matches) > 0:
                    topics.append({word: float(weight) for (weight, word) in matches})
        files[filename] = topics
    for file, topics in files.items():
        num_columns = math.floor(math.sqrt(len(topics)))
        num_rows = math.ceil(len(topics) / num_columns)
        if num_rows == 1 or num_columns == 1:
            plot_1d(topics, num_rows, num_columns, options[file]['title'], PIC_DIR + options[file]['filename'])
        else:
            plot_2d(topics, num_rows, num_columns, options[file]['title'], PIC_DIR + options[file]['filename'])
